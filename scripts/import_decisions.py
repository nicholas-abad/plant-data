"""Upload the review team's decisions into plant_crosswalk — fill-empty-only.

Workflow:
    1. download  SELECT * FROM plant_crosswalk_review   (Neon console → Export CSV)
    2. fill      gem_location_id = L…   or   not_in_gem = true ; leave unknown rows blank;
                 optional free text in decision_note
    3. upload    uv run python scripts/import_decisions.py review.csv --by "C. Team"           # dry run
                 uv run python scripts/import_decisions.py review.csv --by "C. Team" --commit

Rules:
    * Only OPEN rows are touched: no GEM link, not not_in_gem, and no human
      decision (rows the cutover froze as `legacy-pipeline` are open — the
      pipeline decided their values, nobody decided their identity). Rows a
      person already decided are skipped and listed. Changing a decision is a
      separate, deliberate step: --replace "SOURCE|KEY" with a mandatory --note.
    * The table's own guards refuse an unknown GEM ID or a GEM country that
      differs from the source plant's (unless --override-reason is given); each
      refused row is reported with the reason and the rest still go through.
    * Dry run by default. Nothing is written without --commit.

Batch replacement (--replace-file manifest.csv): a reviewed list of exact
old → new changes, e.g. data/crosswalks/decision_migrations/entsoe-gem-2026-10.csv
(columns source_system, key, old_gem_location_id, old_decided_by,
gem_location_id, decided_by, note). A row is replaced only if its current link
and decided_by are exactly the manifest's "old" values (empty = NULL); the run
exits non-zero unless every manifest row was applied, so a partial migration
cannot pass as done. The UPDATE itself re-checks the old values, so a row
changed by someone else after it was read is refused, not overwritten. Keys
are matched verbatim, trailing spaces included (ENTSO-E's 'DEWHV______1_____ '),
unlike the review CSV, whose keys are stripped. The manifest's own note and
decided_by are recorded; --note, --override-reason and --replace do not apply.
verify_crosswalk.py's gate6f accepts the changes the manifests in
decision_migrations/ list, and no others.

Decisions live on the row and survive the weekly rebuild (tier 0 of the
funnel re-emits them). Coordinates, capacity and coal type for a newly linked
row appear after the next rebuild, which derives them from the GEM tables; a
replaced row shows its OLD site's values until then.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import pandas as pd
from loguru import logger
from sqlalchemy import text

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
from bootstrap_neon_db import get_engine  # noqa: E402

KEY_SQL = "source_system = :source_system AND COALESCE(plant_code, plant_name) = :key"
REQUIRED_COLUMNS = {"gem_location_id", "decided_by", "not_in_gem", "source_country"}


def _truthy(v) -> bool:
    return str(v).strip().lower() in {"true", "t", "1", "yes", "y", "x"}


def _clean(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    return s or None


def prepare_rows(df: pd.DataFrame) -> list[dict]:
    """Normalise the team's CSV (as exported from plant_crosswalk_review) into plain dicts.

    Accepts the view's columns (`decision_note`) and the raw table's (`note`).
    Plain dicts, not itertuples: pandas renames underscore-prefixed columns
    positionally in namedtuples, which silently breaks attribute access.
    """
    out = []
    for rec in df.to_dict("records"):
        code = _clean(rec.get("plant_code"))
        name = _clean(rec.get("plant_name"))
        out.append(
            {
                "source_system": _clean(rec.get("source_system")),
                "key": code or name,
                "link": _clean(rec.get("gem_location_id")),
                "nig": _truthy(rec.get("not_in_gem"))
                if _clean(rec.get("not_in_gem"))
                else False,
                "note": _clean(rec.get("decision_note")) or _clean(rec.get("note")),
            }
        )
    return out


def is_open(current) -> bool:
    """A row nobody has decided yet (pipeline-frozen legacy rows count as open)."""
    return (
        current.gem_location_id is None
        and not current.not_in_gem
        and current.decided_by in (None, "legacy-pipeline")
    )


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("csv", type=Path, nargs="?")
    ap.add_argument("--by", required=True, help="who decided (recorded in decided_by)")
    ap.add_argument(
        "--replace-file",
        type=Path,
        metavar="MANIFEST",
        help="apply a reviewed batch of exact old → new replacements (see above)",
    )
    ap.add_argument("--commit", action="store_true", help="write; default is a dry run")
    ap.add_argument(
        "--replace",
        metavar="SOURCE|KEY",
        help="overwrite ONE already-decided row (needs --note)",
    )
    ap.add_argument("--note", help="note stored with the decision(s)")
    ap.add_argument(
        "--override-reason", help="allow a cross-country link, with this reason"
    )
    args = ap.parse_args()
    if args.replace and not args.note:
        ap.error("--replace requires --note")
    if bool(args.csv) == bool(args.replace_file):
        ap.error("give exactly one of a review CSV or --replace-file")
    if args.replace_file:
        if args.replace or args.note or args.override_reason:
            ap.error("--replace, --note and --override-reason do not apply to --replace-file")
        return replace_from_manifest(args)

    df = pd.read_csv(args.csv, dtype=str)
    if not {"source_system", "plant_name"} <= set(df.columns):
        logger.error(
            "CSV must have columns source_system and plant_name (download plant_crosswalk_review)"
        )
        return 2
    rows = prepare_rows(df)
    decided = [r for r in rows if r["link"] or r["nig"]]
    logger.info(
        f"{len(rows):,} rows in file · {len(decided):,} carry a decision · {'COMMIT' if args.commit else 'DRY RUN'}"
    )

    engine = get_engine()
    today = date.today().isoformat()
    filled = skipped = refused = 0
    with engine.begin() as conn:
        have = {
            r[0]
            for r in conn.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND table_name = 'plant_crosswalk'"
                )
            )
        }
        if not REQUIRED_COLUMNS <= have:
            logger.error(
                "plant_crosswalk has no GEM link columns yet — run the crosswalk cutover first"
            )
            return 2
        for r in decided:
            params = {"source_system": r["source_system"], "key": r["key"]}
            target = f"{r['source_system']}|{r['key']}"
            cur = conn.execute(
                text(
                    f"SELECT gem_location_id, not_in_gem, decided_by FROM plant_crosswalk WHERE {KEY_SQL}"
                ),
                params,
            ).fetchone()
            if cur is None:
                refused += 1
                logger.warning(f"refused  {target}: not in plant_crosswalk")
                continue
            if not is_open(cur) and args.replace != target:
                skipped += 1
                logger.info(
                    f"skipped  {target}: already decided by {cur.decided_by} "
                    f"({cur.gem_location_id or 'not in GEM'})"
                )
                continue
            if r["link"] and r["nig"]:
                refused += 1
                logger.warning(f"refused  {target}: both a GEM ID and not_in_gem")
                continue
            gem_name = gem_country = None
            if r["link"]:
                g = conn.execute(
                    text(
                        "SELECT name, country FROM gem_locations WHERE gem_location_id = :L"
                    ),
                    {"L": r["link"]},
                ).fetchone()
                if g:
                    gem_name, gem_country = g.name, g.country
            sp = conn.begin_nested()
            try:
                conn.execute(
                    text(
                        f"""
                        UPDATE plant_crosswalk SET
                            gem_location_id = :L, not_in_gem = :nig, matching_method = 'manual',
                            decided_by = :by, decided_on = :today, note = :note, override_reason = :ovr,
                            gem_name_at_decision = :gname, gem_country_at_decision = :gcountry,
                            candidate_1_id = NULL, candidate_1_name = NULL, candidate_1_score = NULL,
                            candidate_2_id = NULL, candidate_2_name = NULL, candidate_2_score = NULL,
                            candidate_3_id = NULL, candidate_3_name = NULL, candidate_3_score = NULL
                        WHERE {KEY_SQL}"""
                    ),
                    {
                        **params,
                        "L": r["link"],
                        "nig": bool(r["nig"]),
                        "by": args.by,
                        "today": today,
                        "note": args.note or r["note"],
                        "ovr": args.override_reason,
                        "gname": gem_name,
                        "gcountry": gem_country,
                    },
                )
                sp.commit()
                filled += 1
                where = f" ({gem_name}, {gem_country})" if gem_name else ""
                logger.info(f"filled   {target} → {r['link'] or 'not in GEM'}{where}")
            except Exception as e:  # guard trigger refused it
                sp.rollback()
                refused += 1
                msg = str(getattr(e, "orig", e)).splitlines()[0]
                logger.warning(f"refused  {target}: {msg}")
        if not args.commit:
            conn.rollback()
    print(
        f"\n{'committed' if args.commit else 'dry run'}: filled {filled} · skipped {skipped} (already decided) · refused {refused}"
    )
    return 0 if refused == 0 else 1


MANIFEST_COLUMNS = [
    "source_system",
    "key",
    "old_gem_location_id",
    "old_decided_by",
    "gem_location_id",
    "decided_by",
    "note",
]


def replace_from_manifest(args) -> int:
    """Apply every row of a reviewed old → new manifest, or report why not."""
    m = pd.read_csv(args.replace_file, dtype=str, keep_default_na=False)
    missing = set(MANIFEST_COLUMNS) - set(m.columns)
    if missing or m.empty:
        logger.error(f"{args.replace_file}: missing columns {sorted(missing)} or no rows")
        return 2
    if m.duplicated(["source_system", "key"]).any():
        logger.error(f"{args.replace_file}: a key is listed twice")
        return 2
    blank = m[(m[["source_system", "key", "gem_location_id", "decided_by", "note"]] == "").any(axis=1)]
    if len(blank):
        logger.error(f"{args.replace_file}: empty new value in rows {blank.index.tolist()}")
        return 2
    engine = get_engine()
    today = date.today().isoformat()
    applied, refused = [], []
    with engine.begin() as conn:
        for r in m.to_dict("records"):
            target = f"{r['source_system']}|{r['key']}"
            params = {"source_system": r["source_system"], "key": r["key"]}
            cur = conn.execute(
                text(
                    f"SELECT gem_location_id, not_in_gem, decided_by FROM plant_crosswalk WHERE {KEY_SQL}"
                ),
                params,
            ).fetchone()
            if cur is None:
                refused.append((target, "not in plant_crosswalk"))
                continue
            now = (cur.gem_location_id or "", cur.decided_by or "")
            want = (r["old_gem_location_id"], r["old_decided_by"])
            if now != want or cur.not_in_gem:
                refused.append(
                    (target, f"row is {now} (not_in_gem={cur.not_in_gem}), manifest expects {want}")
                )
                continue
            g = conn.execute(
                text("SELECT name, country FROM gem_locations WHERE gem_location_id = :L"),
                {"L": r["gem_location_id"]},
            ).fetchone()
            if g is None:
                refused.append((target, f"{r['gem_location_id']} not in gem_locations"))
                continue
            sp = conn.begin_nested()
            try:
                # the old values again, in the UPDATE itself: a row changed
                # (or deleted) since the SELECT above matches nothing
                done = conn.execute(
                    text(
                        f"""
                        UPDATE plant_crosswalk SET
                            gem_location_id = :L, not_in_gem = FALSE, matching_method = 'manual',
                            decided_by = :by, decided_on = :today, note = :note, override_reason = NULL,
                            gem_name_at_decision = :gname, gem_country_at_decision = :gcountry,
                            candidate_1_id = NULL, candidate_1_name = NULL, candidate_1_score = NULL,
                            candidate_2_id = NULL, candidate_2_name = NULL, candidate_2_score = NULL,
                            candidate_3_id = NULL, candidate_3_name = NULL, candidate_3_score = NULL
                        WHERE {KEY_SQL}
                          AND COALESCE(gem_location_id, '') = :old_L
                          AND COALESCE(decided_by, '') = :old_by
                          AND NOT COALESCE(not_in_gem, FALSE)"""
                    ),
                    {
                        **params,
                        "L": r["gem_location_id"],
                        "by": r["decided_by"],
                        "today": today,
                        "note": f"{r['note']} (applied by {args.by})",
                        "gname": g.name,
                        "gcountry": g.country,
                        "old_L": want[0],
                        "old_by": want[1],
                    },
                )
                if done.rowcount != 1:
                    sp.rollback()
                    refused.append((target, f"row changed while applying ({done.rowcount} rows matched)"))
                    continue
                sp.commit()
                applied.append(target)
                logger.info(f"replaced {target}: {want[0] or 'unlinked'} → {r['gem_location_id']} ({g.name})")
            except Exception as e:  # guard trigger refused it
                sp.rollback()
                refused.append((target, str(getattr(e, "orig", e)).splitlines()[0]))
        for target, why in refused:
            logger.warning(f"refused  {target}: {why}")
        complete = len(applied) == len(m) and not refused
        if not (args.commit and complete):
            conn.rollback()
    print(
        f"\n{'committed' if args.commit and complete else 'NOT committed' if args.commit else 'dry run'}: "
        f"{len(applied)} of {len(m)} manifest rows applicable · {len(refused)} refused"
    )
    return 0 if complete else 1


if __name__ == "__main__":
    sys.exit(main())

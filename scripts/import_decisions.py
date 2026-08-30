"""Upload the review team's decisions into plant_crosswalk — fill-empty-only.

Workflow:
    1. download  SELECT * FROM plant_crosswalk_review   (Neon console → Export CSV)
    2. fill      gem_location_id = L…   or   not_in_gem = true ; leave unknown rows blank
    3. upload    uv run python scripts/import_decisions.py review.csv --by "C. Team"           # dry run
                 uv run python scripts/import_decisions.py review.csv --by "C. Team" --commit

Rules:
    * Only rows whose link columns are still EMPTY in the database are touched.
      A row someone already decided is skipped, so concurrent edits and stale
      files are harmless. Changing a decision is a separate, deliberate step:
      --replace "SOURCE|KEY" with a mandatory --note.
    * The table's own guards refuse an unknown GEM ID or a GEM country that
      differs from the source plant's (unless --override-reason is given); each
      refused row is reported with the reason and the rest still go through.
    * Dry run by default. Nothing is written without --commit.

Decisions live on the row and survive the weekly rebuild (tier 0 of the
funnel re-emits them). Coordinates, capacity and coal type for a newly linked
row appear after the next rebuild, which derives them from the GEM tables.
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


def _truthy(v) -> bool:
    return str(v).strip().lower() in {"true", "t", "1", "yes", "y", "x"}


def _clean(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    return s or None


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("csv", type=Path)
    ap.add_argument("--by", required=True, help="who decided (recorded in decided_by)")
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

    df = pd.read_csv(args.csv, dtype=str)
    need = {"source_system", "plant_name"}
    if not need <= set(df.columns):
        logger.error(
            f"CSV must have columns {sorted(need)} (download plant_crosswalk_review)"
        )
        return 2
    if "plant_code" not in df.columns:
        df["plant_code"] = None
    df["_key"] = df["plant_code"].where(df["plant_code"].notna(), df["plant_name"])
    df["_L"] = df.get("gem_location_id", pd.Series([None] * len(df))).map(_clean)
    df["_nig"] = df.get("not_in_gem", pd.Series([None] * len(df))).map(
        lambda v: _truthy(v) if _clean(v) else False
    )
    df["_note"] = df.get("note", pd.Series([None] * len(df))).map(_clean)
    decided = df[df["_L"].notna() | df["_nig"]]
    logger.info(
        f"{len(df):,} rows in file · {len(decided):,} carry a decision · {'COMMIT' if args.commit else 'DRY RUN'}"
    )

    engine = get_engine()
    today = date.today().isoformat()
    filled = skipped = refused = 0
    with engine.begin() as conn:
        for r in decided.itertuples():
            params = {"source_system": r.source_system, "key": r._key}
            cur = conn.execute(
                text(
                    f"SELECT gem_location_id, not_in_gem, decided_by, source_country FROM plant_crosswalk WHERE {KEY_SQL}"
                ),
                params,
            ).fetchone()
            if cur is None:
                refused += 1
                logger.warning(
                    f"refused  {r.source_system} {r._key!r}: not in plant_crosswalk"
                )
                continue
            is_open = (
                cur.gem_location_id is None
                and not cur.not_in_gem
                and cur.decided_by is None
            )
            target = f"{r.source_system}|{r._key}"
            if not is_open and args.replace != target:
                skipped += 1
                continue
            if r._L and r._nig:
                refused += 1
                logger.warning(f"refused  {target}: both a GEM ID and not_in_gem")
                continue
            gem_name = gem_country = None
            if r._L:
                g = conn.execute(
                    text(
                        "SELECT name, country FROM gem_locations WHERE gem_location_id = :L"
                    ),
                    {"L": r._L},
                ).fetchone()
                if g:
                    gem_name, gem_country = g.name, g.country
            sp = conn.begin_nested()
            try:
                conn.execute(
                    text(f"""
                    UPDATE plant_crosswalk SET
                        gem_location_id = :L, not_in_gem = :nig, matching_method = 'manual',
                        decided_by = :by, decided_on = :today, note = :note, override_reason = :ovr,
                        gem_name_at_decision = :gname, gem_country_at_decision = :gcountry,
                        candidate_1_id = NULL, candidate_1_name = NULL, candidate_1_score = NULL,
                        candidate_2_id = NULL, candidate_2_name = NULL, candidate_2_score = NULL,
                        candidate_3_id = NULL, candidate_3_name = NULL, candidate_3_score = NULL
                    WHERE {KEY_SQL}"""),
                    {
                        **params,
                        "L": r._L,
                        "nig": bool(r._nig),
                        "by": args.by,
                        "today": today,
                        "note": args.note or r._note,
                        "ovr": args.override_reason,
                        "gname": gem_name,
                        "gcountry": gem_country,
                    },
                )
                sp.commit()
                filled += 1
                logger.info(
                    f"filled   {target} → {r._L or 'not in GEM'}{' (' + gem_name + ', ' + gem_country + ')' if gem_name else ''}"
                )
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


if __name__ == "__main__":
    sys.exit(main())

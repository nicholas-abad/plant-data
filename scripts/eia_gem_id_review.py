"""Write the EIA ↔ GEM-code review file, and the decision manifest it implies.

    # 1. list every EIA plant whose link GEM's code would change or add
    uv run python scripts/eia_gem_id_review.py review
    # 2. a person sets verdict = accept | reject | defer (and a note) per row
    # 3. rows tier 0 pins (legacy / GPPD-migration decisions, frozen unlinked
    #    rows) need a decision; write their manifest for import_decisions.py
    uv run python scripts/eia_gem_id_review.py manifest

Both read the LIVE plant_crosswalk (run through run_in_environment.py for the
environment you mean) and data/crosswalks/gem_external_ids.parquet.

`review` keeps the verdict, note, reviewed_by and reviewed_on of rows already
in the file, so re-running it after a GEM or crosswalk change never loses a
person's decision. New rows are pre-filled: `accept` where the automatic rule
holds (GEM gives the code one location, in the mirror, in EIA's state),
`defer` otherwise and for the suspect codes below.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from src import eia_gem_ids as ids_mod  # noqa: E402
from src import gem_reference as gemref  # noqa: E402
from src.build_crosswalk import _make_engine  # noqa: E402
from src.gem_external_ids import eia_plant_locations, load_snapshot  # noqa: E402

MANIFEST_FILE = ids_mod.MIGRATIONS_DIR / "gem-eia-id-2026-10.csv"
DECIDED_BY = "gem-eia-id-2026-10"
PIPELINE_DECIDERS = (
    ids_mod.PIPELINE_DECIDERS
)  # a person's decision is never replaced here

# GEM assertions that pass the automatic rule but look doubtful; a person decides.
SUSPECT = {
    "10307": "GEM location is named 'Toronto Pearson Airport cogeneration complex' (Massachusetts)",
    "55172": "Thad Hill Energy Center at GEM's 'Bosque County Peaking power station' — may be the same site",
    "7552": "Campbell Power Plant at GEM's 'SPA cogeneration 3' — may be the same site",
    "7551": "Procter & Gamble Power Plant at GEM's 'SCA cogeneration 2' — may be the same site",
}


def live_eia(engine) -> pd.DataFrame:
    with engine.connect() as conn:
        xw = pd.read_sql(
            text(
                "SELECT plant_code, plant_name, gem_location_id, matching_method, "
                "decided_by, not_in_gem FROM plant_crosswalk WHERE source_system = 'EIA'"
            ),
            conn,
        )
        st = pd.read_sql(
            text(
                "SELECT plant_code, string_agg(DISTINCT state, '|' ORDER BY state) AS eia_state "
                # the public view of eia_generation_data (same plants and
                # states), so a read-only role can produce the file
                "FROM mv_eia_unit_monthly WHERE plant_code IS NOT NULL GROUP BY 1"
            ),
            conn,
        )
    st["plant_code"] = st["plant_code"].astype(str)
    return xw.merge(st, on="plant_code", how="left")


def loc_name(loc) -> str | None:
    info = gemref.location(loc) if isinstance(loc, str) else None
    return None if info is None else info["name"]


def build_review(
    xw: pd.DataFrame, code_locs: dict[str, set[str]], old: dict
) -> pd.DataFrame:
    rows = []
    for r in xw.itertuples(index=False):
        code = str(r.plant_code)
        locs = code_locs.get(code, set())
        if not locs:
            continue
        rule = ids_mod.rule_target(locs, r.eia_state)
        current = r.gem_location_id if isinstance(r.gem_location_id, str) else None
        if (
            rule.gem_location_id is not None
            and rule.gem_location_id == current
            and code not in SUSPECT
        ):
            continue  # GEM's code confirms today's link
        if rule.gem_location_id is None and current in locs:
            continue  # today's link is one of GEM's locations for the code
        proposed = rule.gem_location_id or (min(locs) if len(locs) == 1 else "")
        verdict = "accept" if rule.gem_location_id and code not in SUSPECT else "defer"
        reason = (
            rule.reason
            if rule.gem_location_id is None
            else ("rule holds" + ("; " + SUSPECT[code] if code in SUSPECT else ""))
        )
        if len(locs) > 1:
            reason += ": " + ", ".join(sorted(locs))
        if bool(r.not_in_gem):
            verdict, reason = (
                "defer",
                reason + "; a person marked this plant not_in_gem",
            )
        decided_by = r.decided_by if isinstance(r.decided_by, str) else ""
        if decided_by and decided_by not in PIPELINE_DECIDERS:
            verdict, reason = "defer", reason + f"; decided by {decided_by}"
        row = {
            "plant_code": code,
            "plant_name": r.plant_name,
            "eia_state": r.eia_state,
            "current_gem_location_id": current or "",
            "current_name": loc_name(current) or "",
            "current_method": r.matching_method
            if isinstance(r.matching_method, str)
            else "",
            "current_decided_by": decided_by,
            "gem_location_id": proposed,
            "gem_name": loc_name(proposed) or "",
            "gem_state": ids_mod.gem_state(proposed) if proposed else "",
            "reason": reason,
            "verdict": verdict,
            "note": "",
            "reviewed_by": "",
            "reviewed_on": "",
        }
        prev = old.get(code)
        if prev is not None and prev.get("reviewed_by"):
            for k in (
                "verdict",
                "note",
                "reviewed_by",
                "reviewed_on",
                "gem_location_id",
            ):
                row[k] = prev[k]
            row["gem_name"] = loc_name(row["gem_location_id"]) or ""
        rows.append(row)
    return pd.DataFrame(rows, columns=ids_mod.REVIEW_COLUMNS).sort_values(
        "plant_code", key=lambda s: s.astype(int)
    )


def build_manifest(xw: pd.DataFrame, review: dict[str, dict]) -> pd.DataFrame:
    """Accepted links on rows tier 0 pins, as exact old → new replacements."""
    rows = []
    for r in xw.itertuples(index=False):
        code = str(r.plant_code)
        rev = review.get(code)
        if rev is None or rev["verdict"] != "accept":
            continue
        if not (isinstance(r.decided_by, str) and r.decided_by in PIPELINE_DECIDERS):
            continue  # pipeline rows: the build's GEM-ID tier relinks them itself
        old = r.gem_location_id if isinstance(r.gem_location_id, str) else ""
        if old == rev["gem_location_id"]:
            continue
        rows.append(
            {
                "source_system": "EIA",
                "key": code,
                "old_gem_location_id": old,
                "old_decided_by": r.decided_by,
                "gem_location_id": rev["gem_location_id"],
                "decided_by": DECIDED_BY,
                "note": f"GEM lists EIA plant code {code} at {rev['gem_location_id']} "
                f"({rev['gem_name']}); was {old or 'unlinked'} ({r.decided_by})"
                + (f". {rev['note']}" if rev.get("note") else ""),
            }
        )
    return pd.DataFrame(rows)


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("action", choices=["review", "manifest"])
    args = ap.parse_args()

    engine = _make_engine()
    gemref.load_tables(engine)
    xw = live_eia(engine)
    if args.action == "review":
        old = ids_mod.load_review()
        out = build_review(xw, eia_plant_locations(load_snapshot()), old)
        out.to_csv(ids_mod.REVIEW_FILE, index=False)
        print(
            f"wrote {len(out)} rows to {ids_mod.REVIEW_FILE}: {out['verdict'].value_counts().to_dict()}"
        )
    else:
        out = build_manifest(xw, ids_mod.load_review())
        MANIFEST_FILE.parent.mkdir(parents=True, exist_ok=True)
        out.to_csv(MANIFEST_FILE, index=False)
        print(f"wrote {len(out)} replacements to {MANIFEST_FILE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

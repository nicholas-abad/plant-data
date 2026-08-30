"""Export the review team's decisions from plant_crosswalk to a CSV in the repo.

Not a source of truth — Neon is (decisions live on the plant_crosswalk row and
the weekly rebuild re-emits them). This file is a human-readable audit trail:
who decided what, when, visible in pull-request diffs. Run weekly by the ETL
workflow after the crosswalk rebuild; commit if changed.

    uv run python scripts/export_decisions.py            # writes data/review/decisions_export.csv
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
from bootstrap_neon_db import get_engine  # noqa: E402

OUT = SCRIPT_DIR.parent / "data" / "review" / "decisions_export.csv"
COLUMNS = [
    "source_system", "plant_code", "plant_name", "source_country", "gem_location_id", "gem_unit_id",
    "not_in_gem", "matching_method", "decided_by", "decided_on", "note", "override_reason",
    "gem_name_at_decision", "gem_country_at_decision",
]


def main() -> int:
    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(
            f"SELECT {', '.join(COLUMNS)} FROM plant_crosswalk "
            "WHERE decided_by IS NOT NULL OR not_in_gem "
            "ORDER BY source_system, plant_name, plant_code",
            conn,
        )
    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT, index=False)
    print(f"{len(df):,} decisions → {OUT.relative_to(SCRIPT_DIR.parent)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

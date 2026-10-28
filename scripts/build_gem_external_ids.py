"""Parse a GEM all-fields export into data/crosswalks/gem_external_ids.parquet.

Usage:
    uv run python scripts/build_gem_external_ids.py "path/to/all-fields-2026-10-13T055958.csv"

The export itself is not committed (75 MB); the parsed IDs are. Record the
export's name in the commit message so the snapshot's date is traceable.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.gem_external_ids import SNAPSHOT_FILE, ids_from_export  # noqa: E402


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__)
        return 2
    ids = ids_from_export(Path(sys.argv[1]))
    ids.to_parquet(SNAPSHOT_FILE, index=False)
    level = ids["gem_unit_id"].isna().map({True: "location", False: "unit"})
    print(f"wrote {len(ids):,} IDs to {SNAPSHOT_FILE}")
    print(
        ids.groupby([level, "namespace"])
        .size()
        .sort_values(ascending=False)
        .head(15)
        .to_string()
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

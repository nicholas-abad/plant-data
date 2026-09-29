"""Extract Climate TRACE's power-plant → GEM links into the committed CSV.

Reads the `gem_ct_crosswalk` tab of Climate TRACE's "download links" workbook
("Climate TRACE download links.xlsx"; its readme dates the tab "November
2025"; received from the client on 2026-09-29 for tracker issue #5). The
workbook itself is not committed — it is mostly raster links for other
sectors. Writes the electricity-generation rows to
data/crosswalks/ct_gem_crosswalk.csv; load that into Neon with
`bootstrap_neon_db.py --ct-gem-only`.

Usage:
    uv run python scripts/extract_ct_gem_crosswalk.py "~/Downloads/Climate TRACE download links.xlsx"
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.ct_gem_crosswalk import SHEET, ct_gem_links_frame

OUT = ROOT / "data" / "crosswalks" / "ct_gem_crosswalk.csv"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("workbook", type=Path)
    args = parser.parse_args()

    raw = pd.read_excel(args.workbook.expanduser(), sheet_name=SHEET, dtype=str)
    links = ct_gem_links_frame(raw)
    links.to_csv(OUT, index=False)
    kinds = links["gem_id_kind"].value_counts().to_dict()
    print(
        f"wrote {OUT.relative_to(ROOT)}: {len(links):,} links, "
        f"{links['climatetrace_id'].nunique():,} Climate TRACE plants, {kinds}"
    )


if __name__ == "__main__":
    main()

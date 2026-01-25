#!/usr/bin/env python3
"""
Download GCPT data from Global Energy Monitor.

Note: The GCPT data requires agreeing to terms of use on the GEM website.
This script provides instructions for manual download.
"""

import sys
from pathlib import Path

from loguru import logger

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils import get_data_dir


def main():
    """Print download instructions for GCPT data."""
    data_dir = get_data_dir() / "gcpt"
    data_dir.mkdir(parents=True, exist_ok=True)

    print("""
================================================================================
                     Global Coal Plant Tracker (GCPT) Download
================================================================================

The GCPT data must be downloaded manually from Global Energy Monitor:

1. Visit: https://globalenergymonitor.org/projects/global-coal-plant-tracker/download-data/

2. Agree to the terms of use (CC BY 4.0 license)

3. Download the Excel file (GCPT Database)

4. Save the file to:
   {data_dir}

5. Rename to: gcpt_global_2025.xlsx
   (or update the year as appropriate)

6. Run the crosswalk builder:
   uv run python scripts/build_crosswalks.py

================================================================================
    """.format(data_dir=data_dir))

    # Check if any GCPT file already exists
    existing = list(data_dir.glob("*.xlsx"))
    if existing:
        print(f"Existing files in {data_dir}:")
        for f in existing:
            print(f"  - {f.name}")
    else:
        print(f"No GCPT files found in {data_dir}")


if __name__ == "__main__":
    main()

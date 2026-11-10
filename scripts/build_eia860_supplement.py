#!/usr/bin/env python3
"""Build the committed supplement of generators retired in older Form 860 editions.

`eia_generator_info` is the 2024 edition plus the retired ('RE') generators
that only older editions list (see src/eia_generator_info.py). The workbooks
are 9-10 MB each and are not committed; this script derives the rows the
loader needs from them:

    data/crosswalks/eia860_retired_2019_2023.csv            the added rows
    data/crosswalks/eia860_retired_2019_2023.manifest.json  where they come from

Inputs:
    data/crosswalks/3_1_Generator_Y2024.xlsx         the base edition (as for the loader)
    data/eia860/<year>/eia860<year>.zip              EIA's archive, 2019-2023;
                                                     downloaded when missing

A downloaded or present zip must have the sha256 the manifest records; pass
--accept-new-archives after EIA re-issued an edition and you checked why.

Usage:
    uv run python scripts/build_eia860_supplement.py           # rebuild both files
    uv run python scripts/build_eia860_supplement.py --check   # fail if they would change
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src import eia_generator_info as register

EDITIONS_DIR = ROOT / "data" / "eia860"
ARCHIVE_URL = "https://www.eia.gov/electricity/data/eia860/archive/xls/eia860{year}.zip"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def workbook_name(year: int) -> str:
    return f"3_1_Generator_Y{year}.xlsx"


def fetch_workbook(year: int, editions_dir: Path) -> tuple[Path, Path]:
    """(zip, generator workbook) of one edition, downloading and extracting as needed."""
    folder = editions_dir / str(year)
    folder.mkdir(parents=True, exist_ok=True)
    archive = folder / f"eia860{year}.zip"
    if not archive.exists():
        url = ARCHIVE_URL.format(year=year)
        print(f"  downloading {url}")
        # EIA's server refuses urllib's default agent.
        request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (plant-data build)"})
        with urllib.request.urlopen(request, timeout=120) as response:
            archive.write_bytes(response.read())
    workbook = folder / workbook_name(year)
    if not workbook.exists():
        with zipfile.ZipFile(archive) as bundle:
            workbook.write_bytes(bundle.read(workbook_name(year)))
    return archive, workbook


def build(editions_dir: Path = EDITIONS_DIR, base_workbook: Path = register.BASE_WORKBOOK):
    """(supplement, manifest dict) from the workbooks on disk."""
    base = register.base_frame(base_workbook)
    older, editions = {}, {}
    for year in register.SUPPLEMENT_EDITIONS:
        archive, workbook = fetch_workbook(year, editions_dir)
        older[year] = register.retired_rows(workbook, year)
        editions[str(year)] = {
            "url": ARCHIVE_URL.format(year=year),
            "zip_sha256": sha256(archive),
            "workbook": workbook.name,
            "workbook_sha256": sha256(workbook),
            "retired_rows": len(older[year]),
        }
    supplement = register.retired_supplement(older, base)
    for year, count in supplement.rows["source_edition"].value_counts().items():
        editions[str(year)]["rows_added"] = int(count)
    manifest = {
        "description": (
            "Form 860 generators with status RE in an older edition's 'Retired and Canceled' sheet "
            "that the base edition lists on neither sheet; newest edition wins. "
            "Built by scripts/build_eia860_supplement.py."
        ),
        "base_edition": {
            "year": register.BASE_EDITION,
            "workbook": base_workbook.name,
            "workbook_sha256": sha256(base_workbook),
            "rows": len(base),
        },
        "editions": editions,
        "rows": len(supplement.rows),
        "coal_rows": int(supplement.rows["technology"].isin(register.COAL_TECHNOLOGIES).sum()),
    }
    return supplement, manifest


def csv_text(rows) -> str:
    return rows.to_csv(index=False, lineterminator="\n")


def manifest_text(manifest: dict) -> str:
    return json.dumps(manifest, indent=2) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--check", action="store_true", help="fail if the committed files would change")
    parser.add_argument(
        "--accept-new-archives",
        action="store_true",
        help="accept an archive whose sha256 differs from the manifest (EIA re-issued it)",
    )
    args = parser.parse_args()

    supplement, manifest = build()
    if len(supplement.technology_changes):
        print(f"\n{len(supplement.technology_changes)} generators change technology between editions:")
        print(supplement.technology_changes.to_string(index=False))

    recorded = json.loads(register.SUPPLEMENT_MANIFEST.read_text()) if register.SUPPLEMENT_MANIFEST.exists() else None
    if recorded and not args.accept_new_archives:
        moved = [
            year
            for year, entry in manifest["editions"].items()
            if recorded["editions"].get(year, {}).get("zip_sha256") not in (None, entry["zip_sha256"])
        ]
        if moved:
            print(f"\nERROR: archives differ from the manifest for {', '.join(moved)}; see --accept-new-archives")
            return 1

    new_csv, new_manifest = csv_text(supplement.rows), manifest_text(manifest)
    if args.check:
        same = (
            register.SUPPLEMENT_FILE.exists()
            and register.SUPPLEMENT_FILE.read_text() == new_csv
            and register.SUPPLEMENT_MANIFEST.read_text() == new_manifest
        )
        print("\nsupplement is up to date" if same else "\nERROR: the committed supplement differs from the workbooks")
        return 0 if same else 1

    register.SUPPLEMENT_FILE.write_text(new_csv)
    register.SUPPLEMENT_MANIFEST.write_text(new_manifest)
    print(
        f"\nwrote {register.SUPPLEMENT_FILE.name}: {manifest['rows']:,} generators "
        f"({manifest['coal_rows']} coal) and {register.SUPPLEMENT_MANIFEST.name}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

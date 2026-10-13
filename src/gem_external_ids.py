"""GEM's external IDs (EIA codes, AEMO DUIDs, …) for its locations and units.

GEM's API does not expose the trackers' "Other IDs" yet (GEM plans it for
around the end of 2026), so the IDs come from a dated GEM export that GEM sent
for this purpose: its ``Non WEPP location IDs`` and ``Non WEPP unit IDs``
columns, e.g. ``"EIA: 6178"`` or ``"Beyond Fossil Fuels: BE-25, ENTSO-E:
22WAMERCO000010Y"``. (WEPP is S&P's database and is not used.)

The parsed snapshot is committed as ``data/crosswalks/gem_external_ids.parquet``
and loaded into the ``gem_external_ids`` table, which keeps the columns
``fetch_gem.py`` created it with. A location-level ID has ``gem_unit_id`` NULL.
``namespace`` is GEM's own label (``EIA``, ``AEMO DUID``, …).
"""

from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path

import pandas as pd

from .utils import get_crosswalk_dir

SNAPSHOT_FILE = get_crosswalk_dir() / "gem_external_ids.parquet"
COLUMNS = ["gem_location_id", "gem_unit_id", "namespace", "external_id"]

# "Kind: value" pairs, comma separated. A kind is any label up to the colon
# ("UDI ID (S&P Global Platts)"); a value runs to the next comma.
_PAIR = re.compile(r"([^,:]+?):\s*([^,]+)")


def parse_ids(cell) -> list[tuple[str, str]]:
    """``"EIA: 6178, EIP: 12"`` → ``[("EIA", "6178"), ("EIP", "12")]``, order kept, duplicates dropped."""
    if not isinstance(cell, str) or not cell.strip():
        return []
    out: list[tuple[str, str]] = []
    for kind, value in _PAIR.findall(cell):
        pair = (kind.strip(), value.strip())
        if pair[0] and pair[1] and pair not in out:
            out.append(pair)
    return out


def ids_from_export(path: Path) -> pd.DataFrame:
    """Every location- and unit-level external ID in a GEM all-fields export."""
    df = pd.read_csv(
        path,
        usecols=["Project ID", "Unit ID", "Non WEPP location IDs", "Non WEPP unit IDs"],
        dtype=str,
        encoding="utf-8-sig",
    )
    df = df.dropna(subset=["Project ID"])
    rows = []
    for loc, cell in df.drop_duplicates("Project ID")[
        ["Project ID", "Non WEPP location IDs"]
    ].itertuples(index=False):
        rows += [(loc, None, k, v) for k, v in parse_ids(cell)]
    for loc, unit, cell in df[
        ["Project ID", "Unit ID", "Non WEPP unit IDs"]
    ].itertuples(index=False):
        if isinstance(unit, str) and unit.strip():
            rows += [(loc, unit, k, v) for k, v in parse_ids(cell)]
    out = pd.DataFrame(rows, columns=COLUMNS).drop_duplicates().reset_index(drop=True)
    # One location's ID cell is repeated on every unit row; a location whose
    # rows disagree would mean the export is not what this parser assumes.
    per_loc = df.groupby("Project ID")["Non WEPP location IDs"].nunique(dropna=False)
    if (per_loc > 1).any():
        raise ValueError(
            f"{int((per_loc > 1).sum())} GEM locations carry different location-ID cells "
            f"on different unit rows, e.g. {per_loc[per_loc > 1].index[:3].tolist()}"
        )
    return out


def load_snapshot(path: Path = SNAPSHOT_FILE) -> pd.DataFrame:
    return pd.read_parquet(path)


def eia_plant_locations(ids: pd.DataFrame) -> dict[str, set[str]]:
    """EIA plant code → every GEM location that lists it."""
    sub = ids[ids["gem_unit_id"].isna() & (ids["namespace"] == "EIA")]
    out: dict[str, set[str]] = defaultdict(set)
    for loc, code in sub[["gem_location_id", "external_id"]].itertuples(index=False):
        out[code].add(loc)
    return dict(out)

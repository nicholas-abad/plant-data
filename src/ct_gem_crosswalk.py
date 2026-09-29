"""Climate TRACE → GEM links, as Climate TRACE publishes them.

Climate TRACE's "download links" workbook carries a `gem_ct_crosswalk` tab
("Crosswalk between Climate TRACE Global Energy Monitor asset IDs (November
2025)") linking each of its assets to Global Energy Monitor IDs. For power
plants (`original_inventory_sector = 'electricity-generation'`) a Climate
TRACE plant maps to one or more GEM **units** (`G…`) and/or **locations**
(`L…`), across every fuel GEM tracks — coal, oil and gas, bioenergy — and
across several GEM locations where GEM splits a complex into phases (Anpara
A/B/C, Paiton).

`plant_crosswalk` cannot hold that: its natural key is one row per Climate
TRACE plant with a single `gem_location_id`. Climate TRACE models the whole
station, so a capacity factor against one coal location understates the
denominator (issue chienleng/global-coal-generation-tracker#5). This table
keeps the links exactly as published; readers resolve `G…` units to their
location through `gem_units` at query time, so a GEM re-fetch never leaves a
stale resolution behind.

**Which GEM units a Climate TRACE plant covers** (`CT_PLANT_UNITS_SQL`):
every OPERATING unit at every GEM location the plant touches — the locations
it names, plus the locations of the units it names — counted once. That is
the whole station, which is what Climate TRACE's generation covers; its sum of
`capacity_mw` is the station capacity. Operating only: counting every status
would add cancelled, planned and retired phases (≈ 6.3 TW instead of ≈ 4.4 TW
across the sheet). A named unit's own location counts even when the plant
names other locations too — that is how complexes split across GEM locations
come in (Datong + Datong-2, Jaworzno, Afşin-Elbistan).

Two tempting alternatives are wrong on this data: summing unit rows and
location rows separately double-counts (most plants carry both, the location
rows repeating the units' locations: ≈ 8.4 TW), and trusting only the units a
plant names undercounts, because the sheet (November 2025) often names only
some of a site's units — 166 of the plants with 12-month Climate TRACE
generation would get less than their current coal capacity.

A plant with no operating unit behind its links gets no rows (616 of the
sheet's 7,335 plants against the current mirror); readers keep their existing
coal-location capacity for it rather than dividing by zero.

Caveat for anyone summing beyond one plant: stations overlap. After the site
expansion, 222 operating units at 74 locations (≈ 54 GW) belong to two
Climate TRACE plants' stations, so per-plant station capacity must not be
added into country totals. `ct_native_source_id` is informational (Climate
TRACE's native id, one per plant in this sheet) and not validated.
"""

from __future__ import annotations

import pandas as pd

SHEET = "gem_ct_crosswalk"
POWER_SECTOR = "electricity-generation"

# Output column order = the committed CSV = the Neon table.
COLUMNS = ["climatetrace_id", "ct_native_source_id", "gem_id", "gem_id_kind"]

_KIND = {"G": "unit", "L": "location"}

# The operating GEM units each Climate TRACE plant covers, one row per (plant,
# unit): operating units at every location the plant names or whose units it
# names. Links naming IDs the mirror lacks drop out. Portable SQL (Postgres
# and SQLite) so the tests run it as written.
CT_PLANT_UNITS_SQL = """
WITH site AS (
  SELECT x.climatetrace_id,
         CASE WHEN x.gem_id_kind = 'location' THEN x.gem_id ELSE u.gem_location_id END
           AS gem_location_id
  FROM ct_gem_crosswalk x
  LEFT JOIN gem_units u ON x.gem_id_kind = 'unit' AND u.gem_unit_id = x.gem_id
)
SELECT DISTINCT s.climatetrace_id, u.gem_unit_id
FROM site s
JOIN gem_units u ON u.gem_location_id = s.gem_location_id
WHERE u.status = 'operating'
"""


def ct_gem_links_frame(raw: pd.DataFrame) -> pd.DataFrame:
    """Power-plant rows of the published sheet, validated and in table shape.

    Accepts either the raw sheet (with `original_inventory_sector`,
    `native_source_id`, `source_id`, `gem_id`) or the committed CSV (already
    in COLUMNS shape). Raises ValueError on anything a reader could misjoin:
    a GEM ID that is neither a unit nor a location, a non-numeric Climate
    TRACE id (the matview's `climatetrace_id` is the numeric source_id), a
    blank id, or a duplicate link. A malformed sheet fails loudly rather than
    being cleaned up.
    """
    df = raw.astype("string")
    if "original_inventory_sector" in df.columns:
        df = df[df["original_inventory_sector"] == POWER_SECTOR]
        df = df.rename(
            columns={
                "source_id": "climatetrace_id",
                "native_source_id": "ct_native_source_id",
            }
        )
    for col in ("climatetrace_id", "ct_native_source_id", "gem_id"):
        # Whitespace-only is blank, not a malformed id.
        df[col] = df[col].str.strip().replace("", pd.NA)

    if df.empty:
        raise ValueError(f"no {POWER_SECTOR} rows")
    if df[["climatetrace_id", "gem_id"]].isna().any().any():
        raise ValueError("rows with a blank climatetrace_id or gem_id")
    bad_id = ~df["climatetrace_id"].str.fullmatch(r"\d+")
    if bad_id.any():
        raise ValueError(
            f"non-numeric climatetrace_id: {df.loc[bad_id, 'climatetrace_id'].head(5).tolist()}"
        )
    bad_gem = ~df["gem_id"].str.fullmatch(r"[GL]\d+")
    if bad_gem.any():
        raise ValueError(
            f"gem_id neither a unit (G…) nor a location (L…): "
            f"{df.loc[bad_gem, 'gem_id'].head(5).tolist()}"
        )
    dupes = df.duplicated(["climatetrace_id", "gem_id"], keep=False)
    if dupes.any():
        raise ValueError(
            f"{int(dupes.sum())} duplicate (climatetrace_id, gem_id) rows, e.g. "
            f"{df.loc[dupes, ['climatetrace_id', 'gem_id']].head(3).values.tolist()}"
        )

    df["gem_id_kind"] = df["gem_id"].str[0].map(_KIND)
    return (
        df[COLUMNS]
        .sort_values(["climatetrace_id", "gem_id"])
        .reset_index(drop=True)
        .astype(object)
        .where(lambda d: d.notna(), None)
    )

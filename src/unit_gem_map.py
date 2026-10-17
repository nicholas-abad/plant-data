"""unit_gem_map: every source's units placed at GEM, in one shape for all countries.

The tracker pairs a feed's rows with GEM's units only through gem_unit_id.
India has its own map (npp_unit_gem_map, unchanged); every other source writes
rows of this shape, and the view gem_unit_monthly attaches them to the
source's monthly generation so the tracker reads one view for all of them
(WHERE gem_location_id = …), as it reads npp_gem_unit_monthly for India.

One row per source unit:
* source_system, plant_key, unit_key: how the source names the unit (EIA:
  plant_code and the keyed generator_id);
* gem_location_id: the GEM plant it belongs to; gem_unit_id: its GEM unit when
  the source row is exactly one unit; gem_unit_ids: every GEM unit the row
  covers, space separated (a phase reporting several units as one total,
  e.g. ONS's Porto do Pecém I = GEM Plant I Units 1 and 2); both NULL for a
  fallback row that only places the unit at its plant's
  crosswalk link (map_source 'crosswalk-plant'), so no generation drops off a
  page while a unit waits for a match;
* fuel / is_coal: the source's own fuel label for the unit and whether the
  tracker counts it as coal, where the label lives with the unit rather than
  the generation row (EIA: Form 860 technology);
* other_gem_unit_ids: a second GEM unit kept aside (a converted unit's
  other-tracker twin, a never-built unit sharing the ID).
"""

from __future__ import annotations

import pandas as pd

from .utils import get_crosswalk_dir

MAP_FILE = get_crosswalk_dir() / "unit_gem_map.parquet"
COLUMNS = [
    "source_system",
    "plant_key",
    "unit_key",
    "gem_location_id",
    "gem_unit_id",
    "gem_unit_ids",
    "map_source",
    "fuel",
    "is_coal",
    "other_gem_unit_ids",
]
FALLBACK = "crosswalk-plant"


def with_fallback(
    matched: pd.DataFrame, units: pd.DataFrame, plant_links: dict[str, str]
) -> pd.DataFrame:
    """Add a plant-level row for every source unit no GEM unit claims.

    ``units`` holds every (source_system, plant_key, unit_key, fuel, is_coal)
    the source reports; ``plant_links`` its plant_key → crosswalk GEM plant.
    A unit whose plant is not linked to GEM gets no row: it is on no page.
    """
    key = ["source_system", "plant_key", "unit_key"]
    rest = units.merge(matched[key], on=key, how="left", indicator=True)
    rest = rest[rest["_merge"] == "left_only"].drop(columns="_merge")
    rest = rest.assign(
        gem_location_id=rest["plant_key"].map(plant_links),
        gem_unit_id=None,
        gem_unit_ids=None,
        map_source=FALLBACK,
        other_gem_unit_ids=None,
    ).dropna(subset=["gem_location_id"])
    out = pd.concat([matched, rest[COLUMNS]], ignore_index=True)[COLUMNS]
    return out.sort_values(key, ignore_index=True)

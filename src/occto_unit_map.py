"""The Japan rows of unit_gem_map: each OCCTO coal unit's GEM unit.

OCCTO reports generation per unit under the plant name ("三隅発電所第１号機",
unit "１号機" = unit No. 1). GEM records no OCCTO identifiers, so within the
GEM plant the crosswalk links the OCCTO plant to, a unit matches the coal unit
GEM numbers the same ("Unit 1"), or the only coal unit when both sides have
one. Cases the numbers do not settle (Takehara's "New Unit 1", Saijo's "Unit 1
Replacement", units GEM files under another plant) come from the reviewed file
data/crosswalks/occto_unit_gem_map.csv. Everything else sits at its plant's
crosswalk link (fallback rows).
"""

from __future__ import annotations

import re
import unicodedata

import pandas as pd
from loguru import logger
from sqlalchemy import text

from .eia_unit_map import NEVER_RAN
from .unit_gem_map import COLUMNS, check_reviewed, with_fallback
from .utils import get_crosswalk_dir

REVIEWED_FILE = get_crosswalk_dir() / "occto_unit_gem_map.csv"
BY_NUMBER = "unit-number"
SINGLE = "single-unit"
REVIEWED = "source-id-reviewed"


def occto_number(unit) -> str | None:
    """'１号機' / '新地1号機' / '6号' → '1' / '1' / '6'; None for '単独' (sole unit)."""
    s = unicodedata.normalize("NFKC", str(unit or "")).strip()
    m = re.search(r"(\d+)\s*号", s) or re.fullmatch(r"\D*(\d+)", s)
    return m.group(1).lstrip("0") or "0" if m else None


def gem_number(name) -> str | None:
    """GEM's 'Unit 1' / 'Unit 3, timepoint1' → '1' / '3'."""
    m = re.search(r"\bUnit\s*(\d+)", str(name or ""))
    return m.group(1) if m else None


def pull_units(engine) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(
            text("SELECT DISTINCT plant, unit, fuel_type FROM mv_occto_unit_monthly WHERE plant IS NOT NULL"),
            conn,
        )


def build_occto_unit_map(
    units: pd.DataFrame, reviewed: pd.DataFrame, gem_units: pd.DataFrame, plant_links: dict[str, str]
) -> pd.DataFrame:
    coal_gem = gem_units[
        (gem_units["tracker"] == "GCPT") & ~gem_units["status"].astype(str).str.lower().isin(NEVER_RAN)
    ]
    by_loc = {loc: grp for loc, grp in coal_gem.groupby("gem_location_id")}
    review = {(r.plant, r.unit): r for r in reviewed.itertuples(index=False)}
    coal = units[units["fuel_type"] == "coal"]
    coal_per_plant = coal.groupby("plant")["unit"].nunique().to_dict()
    rows = []
    for u in coal.itertuples(index=False):
        hit = review.get((u.plant, u.unit))
        if hit is not None:
            unit_ids, loc, how = hit.gem_unit_ids, hit.gem_location_id, REVIEWED
        else:
            loc = plant_links.get(u.plant)
            grp = by_loc.get(loc)
            if grp is None:
                continue
            n = occto_number(u.unit)
            same = grp[grp["unit_name"].map(gem_number) == n] if n else grp.iloc[0:0]
            if len(same) == 1:
                unit_ids, how = same["gem_unit_id"].iloc[0], BY_NUMBER
            elif len(grp) == 1 and coal_per_plant.get(u.plant) == 1:
                unit_ids, how = grp["gem_unit_id"].iloc[0], SINGLE
            else:
                continue
        rows.append(
            {
                "source_system": "OCCTO",
                "plant_key": u.plant,
                "unit_key": u.unit,
                "gem_location_id": loc,
                "gem_unit_id": unit_ids if len(unit_ids.split()) == 1 else None,
                "gem_unit_ids": unit_ids,
                "map_source": how,
                "fuel": "coal",
                "is_coal": True,
                "other_gem_unit_ids": None,
            }
        )
    matched = pd.DataFrame(rows, columns=COLUMNS)
    # An automatic match must not hand one GEM unit to two OCCTO plants (both
    # Hibikinada names sit on a site with one GEM unit): those wait for review.
    auto = matched["map_source"] != REVIEWED
    plants_per_unit = matched[auto].groupby("gem_unit_ids")["plant_key"].nunique()
    shared = set(plants_per_unit[plants_per_unit > 1].index)
    matched = matched[~(auto & matched["gem_unit_ids"].isin(shared))]
    every = pd.DataFrame(
        {
            "source_system": "OCCTO",
            "plant_key": units["plant"],
            "unit_key": units["unit"],
            "fuel": units["fuel_type"],
            "is_coal": units["fuel_type"] == "coal",
        }
    ).drop_duplicates(["plant_key", "unit_key"])
    out = with_fallback(matched, every, plant_links)
    coal_rows = out[out["is_coal"]]
    logger.info(
        f"unit_gem_map OCCTO: coal units on a GEM unit {coal_rows['gem_unit_ids'].notna().sum()}/{len(coal_rows)} "
        f"({(matched['map_source'] == BY_NUMBER).sum()} by number, {(matched['map_source'] == SINGLE).sum()} single, "
        f"{(matched['map_source'] == REVIEWED).sum()} reviewed); {len(out) - len(matched):,} fallback rows"
    )
    return out


def build_from_db(engine, gem_units: pd.DataFrame, crosswalk: pd.DataFrame) -> pd.DataFrame:
    reviewed = pd.read_csv(REVIEWED_FILE, dtype=str, keep_default_na=False)
    check_reviewed(REVIEWED_FILE, reviewed, gem_units, ["plant", "unit"])
    occto = crosswalk[(crosswalk["source_system"] == "OCCTO") & crosswalk["gem_location_id"].notna()]
    return build_occto_unit_map(
        pull_units(engine), reviewed, gem_units, dict(zip(occto["plant_name"], occto["gem_location_id"]))
    )

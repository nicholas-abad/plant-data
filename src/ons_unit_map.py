"""The Brazil rows of unit_gem_map: each ONS coal plant row's GEM unit(s).

ONS reports some coal plants as phases rather than single units (Jorge
Lacerda A/B/C, Porto do Pecém I/II) and renames them over time ("Jorge
Lacerda IV" → "Jorge Lacerda C" → "U.JLACERDA C"), but every row keeps its
ONS ID (SCUTLC). The reviewed file data/crosswalks/ons_unit_gem_map.csv maps
each ONS ID to the GEM unit(s) it covers, checked by capacity; every name the
ID has carried gets that mapping, so a rename keeps its GEM units and the old
and new names read as one series. Coal names without a reviewed ID sit at
their plant's crosswalk link (fallback rows).
"""

from __future__ import annotations

import pandas as pd
from loguru import logger
from sqlalchemy import text

from .unit_gem_map import COLUMNS, with_fallback
from .utils import get_crosswalk_dir

REVIEWED_FILE = get_crosswalk_dir() / "ons_unit_gem_map.csv"
METHOD = "ons-id-reviewed"
COAL = "Carvão"


def load_reviewed(path=REVIEWED_FILE) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    dup = df["ons_plant_id"][df["ons_plant_id"].duplicated()]
    if len(dup):
        raise ValueError(f"{path.name}: ONS IDs listed twice: {dup.tolist()}")
    return df


def check_reviewed(reviewed: pd.DataFrame, units: pd.DataFrame) -> None:
    """Every listed GEM unit exists and sits at the listed GEM plant."""
    where = dict(zip(units["gem_unit_id"], units["gem_location_id"]))
    for r in reviewed.itertuples(index=False):
        for unit in r.gem_unit_ids.split():
            if where.get(unit) != r.gem_location_id:
                raise ValueError(
                    f"{REVIEWED_FILE.name}: {r.ons_plant_id} lists {unit}, which is "
                    f"{'not in gem_units' if unit not in where else 'at ' + where[unit]}, "
                    f"not {r.gem_location_id}"
                )


def build_ons_unit_map(
    names: pd.DataFrame, reviewed: pd.DataFrame, plant_links: dict[str, str]
) -> pd.DataFrame:
    """Rows for every ONS coal name: reviewed GEM unit(s) by ONS ID, else its plant's link.

    ``names``: plant (as mv_ons_plant_monthly names it) and ons_plant_id.
    """
    names = names.drop_duplicates("plant")
    m = names.merge(reviewed, on="ons_plant_id")
    one = m["gem_unit_ids"].str.split().str.len() == 1
    matched = pd.DataFrame(
        {
            "source_system": "ONS",
            "plant_key": m["plant"],
            "unit_key": "",
            "gem_location_id": m["gem_location_id"],
            "gem_unit_id": m["gem_unit_ids"].where(one),
            "gem_unit_ids": m["gem_unit_ids"],
            "map_source": METHOD,
            "fuel": COAL,
            "is_coal": True,
            "other_gem_unit_ids": None,
        },
        columns=COLUMNS,
    )
    every = pd.DataFrame(
        {"source_system": "ONS", "plant_key": names["plant"], "unit_key": "", "fuel": COAL, "is_coal": True}
    )
    out = with_fallback(matched, every, plant_links)
    logger.info(
        f"unit_gem_map ONS: {len(matched):,} coal names placed on reviewed GEM units "
        f"({m['ons_plant_id'].nunique()} ONS IDs), {len(out) - len(matched):,} fallback rows"
    )
    return out


def pull_names(engine) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(
            text(
                "SELECT DISTINCT plant, ons_plant_id FROM ons_generation_data "
                "WHERE fuel_type = :coal AND plant IS NOT NULL"
            ),
            conn,
            params={"coal": COAL},
        )


def build_from_db(engine, units: pd.DataFrame, crosswalk: pd.DataFrame) -> pd.DataFrame:
    reviewed = load_reviewed()
    check_reviewed(reviewed, units)
    ons = crosswalk[(crosswalk["source_system"] == "ONS") & crosswalk["gem_location_id"].notna()]
    return build_ons_unit_map(
        pull_names(engine), reviewed, dict(zip(ons["plant_name"], ons["gem_location_id"]))
    )

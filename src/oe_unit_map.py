"""The Australia rows of unit_gem_map: each OpenElectricity unit's GEM unit.

OE reports every NEM/WEM unit under its market code (DUID: BW01, LYA2,
MUJA_G7) in the unit-grain table ingestion.oe_unit_generation_all_fuels
(ETL migration 022). GEM records the DUID of most Australian units
(gem_external_ids, namespace 'AEMO DUID'), so those match exactly; units GEM
keeps without a DUID (retired ones: Liddell, Muja C Unit 5, Callide A Unit 4)
come from the reviewed file data/crosswalks/oe_unit_gem_map.csv. Units no GEM
unit claims sit at their facility's crosswalk link (fallback rows).
"""

from __future__ import annotations

import pandas as pd
from loguru import logger
from sqlalchemy import text

from .eia_unit_map import NEVER_RAN
from .gem_external_ids import SNAPSHOT_FILE
from .unit_gem_map import COLUMNS, check_reviewed, with_fallback
from .utils import get_crosswalk_dir

REVIEWED_FILE = get_crosswalk_dir() / "oe_unit_gem_map.csv"
DUID = "gem-duid"
REVIEWED = "source-id-reviewed"


def has_unit_table(engine) -> bool:
    with engine.connect() as conn:
        return bool(
            conn.execute(text("SELECT to_regclass('oe_unit_generation_all_fuels') IS NOT NULL")).scalar()
        )


def pull_units(engine) -> pd.DataFrame:
    """Every unit that generated, its facility and fueltech."""
    with engine.connect() as conn:
        return pd.read_sql(
            text(
                "SELECT DISTINCT ON (unit_code) unit_code, facility_name, fueltech "
                "FROM oe_unit_generation_all_fuels WHERE direction = 'injection' "
                "ORDER BY unit_code, timestamp_ms DESC"
            ),
            conn,
        )


def duid_candidates(ids: pd.DataFrame) -> pd.DataFrame:
    duid = ids[(ids["namespace"] == "AEMO DUID") & ids["gem_unit_id"].notna()]
    return pd.DataFrame(
        {
            "unit_code": duid["external_id"].str.upper(),
            "gem_unit_id": duid["gem_unit_id"],
            "gem_location_id": duid["gem_location_id"],
        }
    ).drop_duplicates()


def build_oe_unit_map(
    units: pd.DataFrame,
    candidates: pd.DataFrame,
    reviewed: pd.DataFrame,
    gem_units: pd.DataFrame,
    plant_links: dict[str, str],
) -> pd.DataFrame:
    """Rows for every OE unit: reviewed file first, then a unique GEM DUID, else its facility's link."""
    status = dict(zip(gem_units["gem_unit_id"], gem_units["status"].astype(str).str.lower()))
    where = dict(zip(gem_units["gem_unit_id"], gem_units["gem_location_id"]))
    by_code = reviewed.set_index("unit_code")
    rows = []
    for u in units.itertuples(index=False):
        code = u.unit_code
        if code in by_code.index:
            r = by_code.loc[code]
            unit_ids, loc, how = r["gem_unit_ids"], r["gem_location_id"], REVIEWED
        else:
            c = candidates[(candidates["unit_code"] == code.upper()) & candidates["gem_unit_id"].isin(where)]
            ran = c[~c["gem_unit_id"].map(status).isin(NEVER_RAN)]
            c = ran if len(ran) else c
            if len(c) != 1:
                continue
            unit_ids, loc, how = c["gem_unit_id"].iloc[0], c["gem_location_id"].iloc[0], DUID
        rows.append(
            {
                "source_system": "OE",
                "plant_key": u.facility_name,
                "unit_key": code,
                "gem_location_id": loc,
                "gem_unit_id": unit_ids if len(unit_ids.split()) == 1 else None,
                "gem_unit_ids": unit_ids,
                "map_source": how,
                "fuel": u.fueltech,
                "is_coal": str(u.fueltech).startswith("coal"),
                "other_gem_unit_ids": None,
            }
        )
    matched = pd.DataFrame(rows, columns=COLUMNS)
    every = pd.DataFrame(
        {
            "source_system": "OE",
            "plant_key": units["facility_name"],
            "unit_key": units["unit_code"],
            "fuel": units["fueltech"],
            "is_coal": units["fueltech"].astype(str).str.startswith("coal"),
        }
    )
    out = with_fallback(matched, every, plant_links)
    coal = out[out["is_coal"]]
    logger.info(
        f"unit_gem_map OE: {len(matched):,} units on GEM units "
        f"({(matched['map_source'] == DUID).sum()} by DUID, {(matched['map_source'] == REVIEWED).sum()} reviewed), "
        f"{len(out) - len(matched):,} fallback; coal units on a GEM unit: "
        f"{coal['gem_unit_ids'].notna().sum()}/{len(coal)}"
    )
    return out


def build_from_db(engine, gem_units: pd.DataFrame, crosswalk: pd.DataFrame) -> pd.DataFrame | None:
    if not has_unit_table(engine):
        logger.warning("unit_gem_map OE rows not built: no oe_unit_generation_all_fuels (ETL migration 022)")
        return None
    reviewed = pd.read_csv(REVIEWED_FILE, dtype=str, keep_default_na=False)
    check_reviewed(REVIEWED_FILE, reviewed, gem_units, ["unit_code"])
    oe = crosswalk[(crosswalk["source_system"] == "OE") & crosswalk["gem_location_id"].notna()]
    return build_oe_unit_map(
        pull_units(engine),
        duid_candidates(pd.read_parquet(SNAPSHOT_FILE)),
        reviewed,
        gem_units,
        dict(zip(oe["plant_name"], oe["gem_location_id"])),
    )

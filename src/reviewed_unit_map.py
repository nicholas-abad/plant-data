"""unit_gem_map rows for sources mapped to GEM units by a reviewed file.

Brazil (ONS) and Chile (Coordinador) report coal generation per plant or
phase, under names that change over time, but every row carries a stable
source ID: ONS's plant ID (SCUTLC has been "Jorge Lacerda IV", "Jorge
Lacerda C" and "U.JLACERDA C"), Chile's plant ID. A reviewed file per source
maps each ID to the GEM unit(s) it covers, checked by peak output against
GEM's capacities; every name the ID has carried gets that mapping, so a
rename keeps its units and the old and new names read as one series. Coal
names without a reviewed ID sit at their plant's crosswalk link (fallback).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from loguru import logger
from sqlalchemy import text

from .unit_gem_map import COLUMNS, with_fallback
from .utils import get_crosswalk_dir

METHOD = "source-id-reviewed"


@dataclass(frozen=True)
class Source:
    system: str  # crosswalk source_system
    table: str  # ingestion table holding the names and IDs
    id_column: str
    coal: str  # the source's coal fuel label
    file: Path

    def pull_names(self, engine) -> pd.DataFrame:
        with engine.connect() as conn:
            df = pd.read_sql(
                text(
                    f"SELECT DISTINCT plant, {self.id_column}::text AS source_id FROM {self.table} "
                    "WHERE fuel_type = :coal AND plant IS NOT NULL"
                ),
                conn,
                params={"coal": self.coal},
            )
        return df


SOURCES = [
    Source("ONS", "ons_generation_data", "ons_plant_id", "Carvão", get_crosswalk_dir() / "ons_unit_gem_map.csv"),
    Source("CHILE", "chile_generation_data", "chile_plant_id", "Carbón", get_crosswalk_dir() / "chile_unit_gem_map.csv"),
]  # fmt: skip


def load_reviewed(source: Source) -> pd.DataFrame:
    df = pd.read_csv(source.file, dtype=str, keep_default_na=False)
    df = df.rename(columns={source.id_column: "source_id"})
    dup = df["source_id"][df["source_id"].duplicated()]
    if len(dup):
        raise ValueError(f"{source.file.name}: IDs listed twice: {dup.tolist()}")
    return df


def check_reviewed(source: Source, reviewed: pd.DataFrame, units: pd.DataFrame) -> None:
    """Every listed GEM unit exists and sits at the listed GEM plant."""
    where = dict(zip(units["gem_unit_id"], units["gem_location_id"]))
    for r in reviewed.itertuples(index=False):
        for unit in r.gem_unit_ids.split():
            if where.get(unit) != r.gem_location_id:
                raise ValueError(
                    f"{source.file.name}: {r.source_id} lists {unit}, which is "
                    f"{'not in gem_units' if unit not in where else 'at ' + where[unit]}, "
                    f"not {r.gem_location_id}"
                )


def build_rows(
    source: Source, names: pd.DataFrame, reviewed: pd.DataFrame, plant_links: dict[str, str]
) -> pd.DataFrame:
    """Rows for every coal name: reviewed GEM unit(s) by source ID, else its plant's link.

    ``names``: plant (as the source's monthly view names it) and source_id.
    """
    names = names.drop_duplicates("plant")
    m = names.merge(reviewed, on="source_id")
    one = m["gem_unit_ids"].str.split().str.len() == 1
    matched = pd.DataFrame(
        {
            "source_system": source.system,
            "plant_key": m["plant"],
            "unit_key": "",
            "gem_location_id": m["gem_location_id"],
            "gem_unit_id": m["gem_unit_ids"].where(one),
            "gem_unit_ids": m["gem_unit_ids"],
            "map_source": METHOD,
            "fuel": source.coal,
            "is_coal": True,
            "other_gem_unit_ids": None,
        },
        columns=COLUMNS,
    )
    every = pd.DataFrame(
        {
            "source_system": source.system,
            "plant_key": names["plant"],
            "unit_key": "",
            "fuel": source.coal,
            "is_coal": True,
        }
    )
    out = with_fallback(matched, every, plant_links)
    logger.info(
        f"unit_gem_map {source.system}: {len(matched):,} coal names placed on reviewed GEM units "
        f"({m['source_id'].nunique()} IDs), {len(out) - len(matched):,} fallback rows"
    )
    return out


def build_from_db(engine, source: Source, units: pd.DataFrame, crosswalk: pd.DataFrame) -> pd.DataFrame:
    reviewed = load_reviewed(source)
    check_reviewed(source, reviewed, units)
    linked = crosswalk[(crosswalk["source_system"] == source.system) & crosswalk["gem_location_id"].notna()]
    return build_rows(
        source,
        source.pull_names(engine),
        reviewed,
        dict(zip(linked["plant_name"], linked["gem_location_id"])),
    )

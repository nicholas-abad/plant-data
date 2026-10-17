"""The US rows of unit_gem_map: each EIA generator's GEM unit (see src/unit_gem_map.py).

GEM records, for each US unit, the EIA generator ID (``gem_external_ids``,
unit-level namespace ``EIA``) and, for its location, the EIA plant code. A
generation row (plant_code, generator_id) of mv_eia_unit_monthly is that unit
when both match. GEM spells generator IDs as Form 860 does ("001", "0001"),
generation rows carry eia_generator_key() of EIA's spelling (ETL migration
023), so GEM's IDs go through the same database function before matching.

A generator with several candidate units:
* units that never ran (planned, cancelled) drop out, e.g. Bonanza's cancelled
  "Addition" beside its operating Unit 1, both GEM generator "1";
* a coal unit wins over another tracker's unit for the same generator (coal
  units converted to gas are listed twice by GEM, once per tracker); the other
  is kept in ``other_gem_unit_ids``;
* anything still ambiguous is left unmatched and reported, never guessed.

Generators no GEM unit claims get a fallback row at their plant's crosswalk
link, so the tracker still shows their generation on the right plant.
"""

from __future__ import annotations

import pandas as pd
from loguru import logger
from sqlalchemy import text

from .gem_external_ids import SNAPSHOT_FILE
from .unit_gem_map import COLUMNS, with_fallback

METHOD = "gem-eia-id"
NEVER_RAN = {"planned", "cancelled"}
COAL_TECHNOLOGIES = {"Conventional Steam Coal", "Coal Integrated Gasification Combined Cycle"}


def gem_candidates(ids: pd.DataFrame) -> pd.DataFrame:
    """(plant_code, GEM's generator ID) → GEM unit, from GEM's EIA IDs.

    A location can list several EIA plant codes; its units then pair with each
    of them, and a generator that more than one unit claims stays unmapped.
    """
    eia = ids[ids["namespace"] == "EIA"]
    plants = eia[eia["gem_unit_id"].isna()][["gem_location_id", "external_id"]].rename(
        columns={"external_id": "plant_code"}
    )
    gens = eia[eia["gem_unit_id"].notna()][
        ["gem_location_id", "gem_unit_id", "external_id"]
    ].rename(columns={"external_id": "gem_generator_id"})
    return gens.merge(plants, on="gem_location_id").drop_duplicates()


def key_with_db(engine, frame: pd.DataFrame) -> pd.Series:
    """eia_generator_key(plant_code, gem_generator_id) for every row, computed by the database."""
    with engine.connect() as conn:
        keys = conn.execute(
            text(
                "SELECT public.eia_generator_key(p, g) FROM unnest(CAST(:p AS text[]), CAST(:g AS text[])) "
                "WITH ORDINALITY AS x(p, g, n) ORDER BY n"
            ),
            {
                "p": frame["plant_code"].astype(str).tolist(),
                "g": frame["gem_generator_id"].astype(str).tolist(),
            },
        ).scalars()
        return pd.Series(list(keys), index=frame.index)


def has_key_function(engine) -> bool:
    with engine.connect() as conn:
        return bool(
            conn.execute(text("SELECT to_regproc('public.eia_generator_key') IS NOT NULL")).scalar()
        )


def choose(cands: pd.DataFrame) -> tuple[pd.Series | None, str]:
    """The unit for one generator, or None and why not.

    ``cands`` has gem_unit_id, gem_location_id, tracker and status for every
    unit GEM gives this generator's (plant_code, generator_id).
    """
    cands = cands.drop_duplicates("gem_unit_id")
    ran = cands[~cands["status"].astype(str).str.lower().isin(NEVER_RAN)]
    if not ran.empty:
        cands = ran
    if len(cands) == 1:
        return cands.iloc[0], ""
    coal = cands[cands["tracker"] == "GCPT"]
    if len(coal) == 1:
        return coal.iloc[0], ""
    return None, f"{len(cands)} GEM units claim it: {', '.join(sorted(cands['gem_unit_id']))}"


def build_eia_unit_map(
    generators: pd.DataFrame, candidates: pd.DataFrame, units: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """(map, unmapped) for every generator in ``generators`` that GEM's IDs reach.

    ``generators``: plant_code, generator_id (keyed), technology of mv_eia_unit_monthly.
    ``candidates``: plant_code, generator_id (keyed), gem_unit_id, gem_location_id.
    ``units``: gem_units with gem_unit_id, tracker, status.
    """
    # GEM's export can be newer than the gem_units mirror; a unit the mirror
    # lacks has no status or tracker to choose by, so it waits for the mirror.
    c = candidates.merge(units[["gem_unit_id", "tracker", "status"]], on="gem_unit_id")
    if len(c) < len(candidates):
        logger.info(
            f"unit_gem_map EIA: {candidates['gem_unit_id'].nunique() - c['gem_unit_id'].nunique():,} "
            "GEM units in the ID snapshot are not in gem_units yet; skipped"
        )
    c = c.merge(
        generators[["plant_code", "generator_id", "technology"]].drop_duplicates(["plant_code", "generator_id"]),
        on=["plant_code", "generator_id"],
    )
    rows, unmapped = [], []
    for (plant, gen), grp in c.groupby(["plant_code", "generator_id"], sort=True):
        pick, why = choose(grp)
        if pick is None:
            unmapped.append({"plant_code": plant, "generator_id": gen, "reason": why})
            continue
        others = sorted(set(grp["gem_unit_id"]) - {pick["gem_unit_id"]})
        rows.append(
            {
                "source_system": "EIA",
                "plant_key": plant,
                "unit_key": gen,
                "gem_location_id": pick["gem_location_id"],
                "gem_unit_id": pick["gem_unit_id"],
                "gem_unit_ids": pick["gem_unit_id"],
                "map_source": METHOD,
                "fuel": pick["technology"] if isinstance(pick["technology"], str) else None,
                "is_coal": pick["technology"] in COAL_TECHNOLOGIES,
                "other_gem_unit_ids": ", ".join(others) or None,
            }
        )
    out = pd.DataFrame(rows, columns=COLUMNS)
    miss = pd.DataFrame(unmapped, columns=["plant_code", "generator_id", "reason"])
    logger.info(
        f"unit_gem_map EIA: {len(out):,} generators matched to a GEM unit "
        f"({out['other_gem_unit_ids'].notna().sum():,} with a second GEM unit kept aside); "
        f"{len(miss):,} left unmatched as ambiguous"
    )
    return out, miss


def pull_generators(engine) -> pd.DataFrame:
    """Every generator with generation, and its Form 860 technology (keyed join)."""
    with engine.connect() as conn:
        return pd.read_sql(
            text(
                "SELECT DISTINCT e.plant_code::text AS plant_code, e.generator_id, i.technology "
                "FROM mv_eia_unit_monthly e LEFT JOIN eia_generator_info i "
                "ON i.plant_code = e.plant_code AND i.generator_id = e.generator_id"
            ),
            conn,
        )


def build_from_db(
    engine, units: pd.DataFrame, crosswalk: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame] | None:
    """The US rows (matched + fallback) and the ambiguous generators; None without migration 023."""
    if not has_key_function(engine):
        logger.warning(
            "unit_gem_map EIA rows not built: the database lacks eia_generator_key() (ETL migration 023)"
        )
        return None
    cands = gem_candidates(pd.read_parquet(SNAPSHOT_FILE))
    cands["generator_id"] = key_with_db(engine, cands)
    gens = pull_generators(engine)
    matched, miss = build_eia_unit_map(gens, cands, units)
    every = pd.DataFrame(
        {
            "source_system": "EIA",
            "plant_key": gens["plant_code"],
            "unit_key": gens["generator_id"],
            "fuel": gens["technology"],
            "is_coal": gens["technology"].isin(COAL_TECHNOLOGIES),
        }
    ).drop_duplicates(["plant_key", "unit_key"])
    eia = crosswalk[(crosswalk["source_system"] == "EIA") & crosswalk["gem_location_id"].notna()]
    out = with_fallback(matched, every, dict(zip(eia["plant_code"].astype(str), eia["gem_location_id"])))
    logger.info(
        f"unit_gem_map EIA: {(out['map_source'] == METHOD).sum():,} matched + "
        f"{(out['map_source'] != METHOD).sum():,} fallback rows at their plant's GEM plant"
    )
    return out, miss

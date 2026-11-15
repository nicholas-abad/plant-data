"""The US rows of unit_gem_map: each EIA generator's GEM unit (see src/unit_gem_map.py).

GEM records, for each US unit, the EIA generator ID (``gem_external_ids``,
unit-level namespace ``EIA``) and, for its location, the EIA plant code. A
generation row (plant_code, generator_id) of mv_eia_unit_monthly is that unit
when both match. GEM spells generator IDs as Form 860 does ("001", "0001"),
generation rows carry eia_generator_key() of EIA's spelling (ETL migration
023), so GEM's IDs go through the same database function before matching.

A generator with several candidate units:
* units that never ran (planned, cancelled) drop out, e.g. Bonanza's cancelled
  "Addition" beside its operating Unit 1, both GEM generator "1"; a generator
  only such units claim stays unmatched;
* a coal unit wins over another tracker's unit for the same generator (coal
  units converted to gas are listed twice by GEM, once per tracker); the other
  is kept in ``other_gem_unit_ids``;
* anything still ambiguous is left unmatched and reported, never guessed;
* a GEM unit chosen by generators at two plant codes (a location GEM lists
  under several codes) places neither: one unit cannot carry two generators.

GEM's location codes go through the same review as the plant links
(``eia_gem_id_review.csv``, src/eia_gem_ids.py): a code rejected or deferred
there places none of its units, and an accepted code places units only at the
accepted location.

Generators no GEM unit claims get a fallback row at their plant's crosswalk
link, so the tracker still shows their generation on the right plant.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from loguru import logger
from sqlalchemy import text

from . import eia_gem_ids
from .eia_generator_info import COAL_TECHNOLOGIES
from .gem_external_ids import SNAPSHOT_FILE
from .unit_gem_map import COLUMNS, with_fallback
from .utils import get_crosswalk_dir

METHOD = "gem-eia-id"
NEVER_RAN = {"planned", "cancelled"}
# Generators known to have generation but no Form 860 row (see no_fuel_findings).
NO_FUEL_EXCEPTIONS_FILE = get_crosswalk_dir() / "eia_no_fuel_exceptions.csv"


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


def apply_review(cands: pd.DataFrame, review: dict[str, dict]) -> pd.DataFrame:
    """Drop candidates the plant-code review does not allow (see the module docstring)."""
    verdict = cands["plant_code"].map(lambda c: review.get(c, {}).get("verdict"))
    accepted = cands["plant_code"].map(lambda c: review.get(c, {}).get("gem_location_id"))
    keep = verdict.isna() | ((verdict == "accept") & (cands["gem_location_id"] == accepted))
    dropped = cands.loc[~keep, "plant_code"].nunique()
    if dropped:
        logger.info(f"unit_gem_map EIA: {dropped} plant codes held back by eia_gem_id_review.csv")
    return cands[keep]


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
    cands = cands[~cands["status"].astype(str).str.lower().isin(NEVER_RAN)]
    if cands.empty:
        return None, "only units that never ran (planned, cancelled) claim it"
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
    shared = out.groupby("gem_unit_id")["plant_key"].transform("nunique") > 1
    claimants = out[shared].groupby("gem_unit_id")["plant_key"].agg(lambda s: ", ".join(sorted(set(s))))
    for r in out[shared].itertuples(index=False):
        unmapped.append(
            {
                "plant_code": r.plant_key,
                "generator_id": r.unit_key,
                "reason": f"{r.gem_unit_id} is also chosen at plant codes {claimants[r.gem_unit_id]}",
            }
        )
    out = out[~shared].reset_index(drop=True)
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
    cands = apply_review(gem_candidates(pd.read_parquet(SNAPSHOT_FILE)), eia_gem_ids.load_review())
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


@dataclass(frozen=True)
class NoFuelFindings:
    """US unit-map generators without a Form 860 technology, by what to do about them.

    Each entry reads "plant_code/generator_id (first generation YYYY-MM)".
    """

    # Generation starts within the loaded register's years and nothing explains
    # the missing row: the register is incomplete, and the unit would silently
    # count as "not coal". The gate fails.
    failing: list[str]
    # Generation starts after the loaded register's year: commissioned since,
    # expected every year until the next Form 860 is loaded. Warn only.
    new: list[str]
    # Listed in eia_no_fuel_exceptions.csv: known, with a written reason.
    skipped: list[str]
    # Listed there but has a technology by now: the row can go. Warn only.
    stale: list[str]


def load_no_fuel_exceptions(path: Path = NO_FUEL_EXCEPTIONS_FILE) -> set[tuple[str, str]]:
    """(plant_code, generator key) of every generator the no-fuel guard skips.

    ``generator_id`` in the file is the generation key (unit_gem_map.unit_key),
    which is not always Form 860's spelling: Blue Water is "IS" in Form 923.
    """
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    blank = df[(df["reason"].str.strip() == "") | (df["plant_code"] == "") | (df["generator_id"] == "")]
    if len(blank):
        raise ValueError(f"{path.name}: {len(blank)} rows lack a plant_code, generator_id or reason")
    dup = df[df.duplicated(["plant_code", "generator_id"])]
    if len(dup):
        raise ValueError(f"{path.name}: listed twice: {dup[['plant_code', 'generator_id']].values.tolist()}")
    return set(zip(df["plant_code"], df["generator_id"]))


def no_fuel_findings(
    eia_rows: pd.DataFrame,
    first_month: dict[tuple[str, str], dt.date],
    exceptions: set[tuple[str, str]],
    register_year_end: dt.date,
) -> NoFuelFindings:
    """Sort the US unit-map rows that have no fuel (see NoFuelFindings).

    ``eia_rows``: unit_gem_map's EIA rows (plant_key, unit_key, fuel).
    ``first_month``: each generator's first month in mv_eia_unit_monthly.
    A listed generator that is not in the map at all is not reported: its
    plant is simply not linked to GEM (yet).
    """
    failing, new, skipped, stale = [], [], [], []
    for plant, unit, fuel in zip(eia_rows["plant_key"], eia_rows["unit_key"], eia_rows["fuel"]):
        key = (plant, unit)
        first = first_month.get(key)
        label = f"{plant}/{unit} (first generation {first:%Y-%m})" if first else f"{plant}/{unit}"
        if isinstance(fuel, str) and fuel:
            if key in exceptions:
                stale.append(label)
        elif key in exceptions:
            skipped.append(label)
        elif first is not None and first > register_year_end:
            new.append(label)
        else:
            # Unknown first month counts as old: never excuse what cannot be dated.
            failing.append(label)
    return NoFuelFindings(sorted(failing), sorted(new), sorted(skipped), sorted(stale))

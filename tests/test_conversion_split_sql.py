"""gem_unit_monthly's EIA and OCCTO branches split a converted unit by month (tracker #10).

Runs the loader's own branch SQL on a local throwaway database (tests/conftest.py).
"""

import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from bootstrap_neon_db import GEM_UNIT_BRANCHES  # noqa: E402

SCHEMA = [
    "CREATE TABLE mv_eia_unit_monthly (plant_code text, generator_id text, month timestamptz, "
    "timestamp_ms bigint, net_generation_mwh double precision)",
    "CREATE TABLE mv_occto_unit_monthly (plant text, unit text, month timestamptz, fuel_type text, "
    "generation_mwh double precision)",
    "CREATE TABLE unit_gem_map (source_system text, plant_key text, unit_key text, gem_location_id text, "
    "gem_unit_id text, gem_unit_ids text, map_source text, fuel text, is_coal boolean, other_gem_unit_ids text, "
    "converted_gem_unit_id text, coal_until_month text, boundary_source text, fuel_before text, fuel_after text)",
    # Montour 1: Form 860 still says coal; coal until 2024-01, then gas on its twin.
    "INSERT INTO unit_gem_map VALUES ('EIA','3149','1','L_M','G_M1','G_M1','gem-eia-id','Conventional Steam Coal',"
    "true,NULL,'G_M1_GAS','2024-01','reviewed','Conventional Steam Coal','Natural Gas Steam Turbine')",
    # An ordinary US coal unit, no boundary.
    "INSERT INTO unit_gem_map VALUES ('EIA','1','1','L_A','G_A','G_A','gem-eia-id','Conventional Steam Coal',"
    "true,NULL,NULL,NULL,NULL,NULL,NULL)",
    # Suzukawa 1: OCCTO still labels it coal; GEM's year 2022, so every loaded month is biomass on the twin.
    "INSERT INTO unit_gem_map VALUES ('OCCTO','鈴川','単独','L_S','G_S','G_S','single-unit','coal',true,NULL,"
    "'G_S_BIO','2022-12','gem-year-end','coal','biomass')",
    # Ishinomaki Hibarino 1: never coal, no unit (plant fallback), no twin.
    "INSERT INTO unit_gem_map VALUES ('OCCTO','石巻','1号機','L_I',NULL,NULL,'crosswalk-plant','coal',true,NULL,"
    "NULL,NULL,'never-coal','coal','biomass')",
    # An OCCTO gas unit without a boundary stays out, as before.
    "INSERT INTO unit_gem_map VALUES ('OCCTO','某','2号機','L_X',NULL,NULL,'crosswalk-plant','gas',false,NULL,"
    "NULL,NULL,NULL,NULL,NULL)",
    "INSERT INTO mv_eia_unit_monthly VALUES "
    "('3149','1','2023-12-01T00:00:00Z',1701388800000,100),('3149','1','2024-01-01T00:00:00Z',1704067200000,90),"
    "('3149','1','2024-02-01T00:00:00Z',1706745600000,80),('1','1','2024-02-01T00:00:00Z',1706745600000,50)",
    "INSERT INTO mv_occto_unit_monthly VALUES "
    "('鈴川','単独','2024-03-01T00:00:00Z','coal',30),('石巻','1号機','2024-03-01T00:00:00Z','coal',20),"
    "('某','2号機','2024-03-01T00:00:00Z','gas',10)",
]


def _rows(pg_url) -> pd.DataFrame:
    engine = create_engine(pg_url)
    with engine.begin() as conn:
        for stmt in SCHEMA:
            conn.execute(text(stmt))
        union = " UNION ALL ".join(
            f"({GEM_UNIT_BRANCHES[v]})"
            for v in ("mv_eia_unit_monthly", "mv_occto_unit_monthly")
        )
        out = pd.read_sql(text(f"SELECT * FROM ({union}) v"), conn)
    engine.dispose()
    out["m"] = pd.to_datetime(out["month"], utc=True).dt.strftime("%Y-%m")
    return out.set_index(["plant_key", "m"])


def test_months_up_to_the_last_coal_month_are_coal_on_the_coal_unit(pg_url):
    v = _rows(pg_url)
    for month in ("2023-12", "2024-01"):
        r = v.loc[("3149", month)]
        assert (bool(r.is_coal), r.gem_unit_id, r.gem_unit_ids, r.fuel) == (
            True, "G_M1", "G_M1", "Conventional Steam Coal",
        )  # fmt: skip


def test_months_after_it_are_the_other_fuel_on_the_twin(pg_url):
    r = _rows(pg_url).loc[("3149", "2024-02")]
    assert (bool(r.is_coal), r.gem_unit_id, r.gem_unit_ids, r.fuel) == (
        False, "G_M1_GAS", "G_M1_GAS", "Natural Gas Steam Turbine",
    )  # fmt: skip


def test_a_unit_without_a_boundary_is_unchanged(pg_url):
    r = _rows(pg_url).loc[("1", "2024-02")]
    assert (bool(r.is_coal), r.gem_unit_id, r.fuel) == (
        True,
        "G_A",
        "Conventional Steam Coal",
    )


def test_a_converted_occto_unit_labelled_coal_shows_as_biomass_on_its_twin(pg_url):
    r = _rows(pg_url).loc[("鈴川", "2024-03")]
    assert (bool(r.is_coal), r.gem_unit_id, r.fuel) == (False, "G_S_BIO", "biomass")


def test_a_never_coal_unit_stays_visible_and_is_never_coal(pg_url):
    r = _rows(pg_url).loc[("石巻", "2024-03")]
    assert (bool(r.is_coal), r.fuel) == (False, "biomass")
    assert pd.isna(r.gem_unit_id)


def test_each_source_month_once_and_no_mwh_added_or_lost(pg_url):
    v = _rows(pg_url)
    assert not v.index.duplicated().any()
    assert (
        v["generation_mwh"].sum() == 100 + 90 + 80 + 50 + 30 + 20
    )  # the gas unit without a boundary stays out
    assert "某" not in v.index.get_level_values("plant_key")

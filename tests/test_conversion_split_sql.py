"""gem_unit_monthly's EIA and OCCTO branches split a converted unit by month (tracker #10).

Runs the loader's own branch SQL on a local throwaway database (tests/conftest.py).
"""

import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from bootstrap_neon_db import GEM_UNIT_BRANCHES

MAP_COLUMNS = (
    "source_system, plant_key, unit_key, gem_location_id, gem_unit_id, gem_unit_ids, map_source, fuel, is_coal, "
    "other_gem_unit_ids, converted_gem_unit_id, coal_until_month, noncoal_months, boundary_source, fuel_before, "
    "fuel_after"
)
COAL, GAS = "Conventional Steam Coal", "Natural Gas Steam Turbine"
# (source, plant, unit, location, unit id, map_source, fuel, is_coal, twin, last coal month, non-coal months,
#  boundary_source, fuel before, fuel after)
MAP = [
    # Montour 1: Form 860 still says coal; coal until 2024-01, then gas on its twin.
    ("EIA", "3149", "1", "L_M", "G_M1", "gem-eia-id", COAL, True, "G_M1_GAS", "2024-01", None, "reviewed", COAL, GAS),
    # Montour 2: gas in 2023-10, coal again until 2024-02.
    ("EIA", "3149", "2", "L_M", "G_M2", "gem-eia-id", GAS, False, "G_M2_GAS", "2024-02", "2023-10", "reviewed", COAL, GAS),
    # An ordinary US coal unit, no boundary.
    ("EIA", "1", "1", "L_A", "G_A", "gem-eia-id", COAL, True, None, None, None, None, None, None),
    # A reviewed month G-CONVERSION reports (no twin): not applied, the unit stays as today.
    ("EIA", "2", "1", "L_U", "G_U", "gem-eia-id", COAL, True, None, "2023-01", None, "unresolved", COAL, None),
    # Suzukawa 1: OCCTO still labels it coal; GEM's year 2022, so every loaded month is biomass on the twin.
    ("OCCTO", "鈴川", "単独", "L_S", "G_S", "single-unit", "coal", True, "G_S_BIO", "2022-12", None, "gem-year-end", "coal", "biomass"),
    # Ishinomaki Hibarino 1: never coal, no unit (plant fallback), no twin.
    ("OCCTO", "石巻", "1号機", "L_I", None, "crosswalk-plant", "coal", True, None, None, None, "never-coal", "coal", "biomass"),
    # A converted OCCTO unit before its boundary: a month OCCTO labels gas is not coal.
    ("OCCTO", "某発電所", "1号機", "L_Y", "G_Y", "unit-number", "coal", True, "G_Y_GAS", "2026-12", None, "gem-year-end", "coal", "gas"),
    # An OCCTO gas unit without a boundary stays out, as before.
    ("OCCTO", "某", "2号機", "L_X", None, "crosswalk-plant", "gas", False, None, None, None, None, None, None),
]  # fmt: skip
EIA_ROWS = [
    ("3149", "1", "2023-12", 100), ("3149", "1", "2024-01", 90), ("3149", "1", "2024-02", 80),
    ("3149", "2", "2023-10", 70), ("3149", "2", "2024-01", 60),
    ("1", "1", "2024-02", 50), ("2", "1", "2024-02", 40),
]  # fmt: skip
OCCTO_ROWS = [
    ("鈴川", "単独", "2024-03", "coal", 30), ("石巻", "1号機", "2024-03", "coal", 20),
    ("某発電所", "1号機", "2024-03", "gas", 15), ("某発電所", "1号機", "2024-04", "coal", 12),
    ("某", "2号機", "2024-03", "gas", 10),
]  # fmt: skip


def _rows(pg_url) -> pd.DataFrame:
    engine = create_engine(pg_url)
    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE mv_eia_unit_monthly (plant_code text, generator_id text, month timestamptz, "
            "timestamp_ms bigint, net_generation_mwh double precision)"
        ))  # fmt: skip
        conn.execute(text(
            "CREATE TABLE mv_occto_unit_monthly (plant text, unit text, month timestamptz, fuel_type text, "
            "generation_mwh double precision)"
        ))  # fmt: skip
        conn.execute(
            text(
                f"CREATE TABLE unit_gem_map ({', '.join(c + ' text' for c in MAP_COLUMNS.split(', '))})"
            )
        )
        conn.execute(
            text(
                "ALTER TABLE unit_gem_map ALTER COLUMN is_coal TYPE boolean USING is_coal::boolean"
            )
        )
        for r in MAP:
            (
                source,
                plant,
                unit,
                loc,
                gem,
                how,
                fuel,
                coal,
                twin,
                until,
                gaps,
                where,
                before,
                after,
            ) = r
            conn.execute(
                text(f"INSERT INTO unit_gem_map ({MAP_COLUMNS}) VALUES "
                     "(:s, :p, :u, :l, :g, :g, :h, :f, :c, NULL, :t, :until, :gaps, :b, :fb, :fa)"),
                dict(s=source, p=plant, u=unit, l=loc, g=gem, h=how, f=fuel, c=coal, t=twin, until=until,
                     gaps=gaps, b=where, fb=before, fa=after),
            )  # fmt: skip
        for plant, unit, month, mwh in EIA_ROWS:
            conn.execute(
                text("INSERT INTO mv_eia_unit_monthly VALUES (:p, :u, CAST(:m AS timestamptz), "
                     "(EXTRACT(EPOCH FROM CAST(:m AS timestamptz)) * 1000)::bigint, :w)"),
                dict(p=plant, u=unit, m=f"{month}-01T00:00:00Z", w=mwh),
            )  # fmt: skip
        for plant, unit, month, fuel, mwh in OCCTO_ROWS:
            conn.execute(
                text(
                    "INSERT INTO mv_occto_unit_monthly VALUES (:p, :u, CAST(:m AS timestamptz), :f, :w)"
                ),
                dict(p=plant, u=unit, m=f"{month}-01T00:00:00Z", f=fuel, w=mwh),
            )
        union = " UNION ALL ".join(
            f"({GEM_UNIT_BRANCHES[v]})"
            for v in ("mv_eia_unit_monthly", "mv_occto_unit_monthly")
        )
        out = pd.read_sql(text(f"SELECT * FROM ({union}) v"), conn)
    engine.dispose()
    out["m"] = pd.to_datetime(out["month"], utc=True).dt.strftime("%Y-%m")
    return out.set_index(["plant_key", "unit_key", "m"])


def _r(v, plant, unit, month):
    r = v.loc[(plant, unit, month)]
    return bool(r.is_coal), r.gem_unit_id, r.fuel


def test_months_up_to_the_last_coal_month_are_coal_on_the_coal_unit(pg_url):
    v = _rows(pg_url)
    assert (
        _r(v, "3149", "1", "2023-12")
        == _r(v, "3149", "1", "2024-01")
        == (True, "G_M1", COAL)
    )


def test_months_after_it_are_the_other_fuel_on_the_twin(pg_url):
    v = _rows(pg_url)
    assert _r(v, "3149", "1", "2024-02") == (False, "G_M1_GAS", GAS)
    assert v.loc[("3149", "1", "2024-02"), "gem_unit_ids"] == "G_M1_GAS"


def test_a_listed_non_coal_month_before_the_boundary_is_not_coal_on_the_coal_unit(
    pg_url,
):
    v = _rows(pg_url)
    assert _r(v, "3149", "2", "2023-10") == (False, "G_M2", GAS)
    assert _r(v, "3149", "2", "2024-01") == (
        True,
        "G_M2",
        COAL,
    )  # Form 860 says gas, the boiler said coal


def test_units_without_an_applied_boundary_are_unchanged(pg_url):
    v = _rows(pg_url)
    assert _r(v, "1", "1", "2024-02") == (True, "G_A", COAL)
    assert _r(v, "2", "1", "2024-02") == (
        True,
        "G_U",
        COAL,
    )  # unresolved: reported by the gate, not applied


def test_a_converted_occto_unit_labelled_coal_shows_as_biomass_on_its_twin(pg_url):
    assert _r(_rows(pg_url), "鈴川", "単独", "2024-03") == (False, "G_S_BIO", "biomass")


def test_an_occto_month_labelled_other_than_coal_before_the_boundary_stays_not_coal(
    pg_url,
):
    v = _rows(pg_url)
    assert _r(v, "某発電所", "1号機", "2024-03") == (False, "G_Y", "gas")
    assert _r(v, "某発電所", "1号機", "2024-04") == (True, "G_Y", "coal")


def test_a_never_coal_unit_stays_visible_and_is_never_coal(pg_url):
    coal, unit, fuel = _r(_rows(pg_url), "石巻", "1号機", "2024-03")
    assert (coal, fuel) == (False, "biomass") and pd.isna(unit)


def test_each_source_month_once_and_no_mwh_added_or_lost(pg_url):
    v = _rows(pg_url)
    assert not v.index.duplicated().any()
    assert v["generation_mwh"].sum() == sum(r[3] for r in EIA_ROWS) + sum(
        r[4] for r in OCCTO_ROWS[:4]
    )
    assert "某" not in v.index.get_level_values(
        "plant_key"
    )  # the gas unit without a boundary stays out


def test_the_view_applies_exactly_the_active_boundaries():
    from bootstrap_neon_db import ACTIVE_BOUNDARIES

    from src.conversion_boundaries import ACTIVE

    assert ACTIVE_BOUNDARIES == tuple(str(s) for s in ACTIVE)

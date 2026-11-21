"""G-CONVERSION (scripts/verify_crosswalk.py): the month split is checked before a map is loaded (tracker #10).

Runs on a local throwaway database (tests/conftest.py); the gate shadows
unit_gem_map with the candidate map and rolls back.
"""

import importlib.util
import sys
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from src import conversion_boundaries
from src.conversion_boundaries import BOUNDARY_COLUMNS, BoundaryDecision
from src.unit_gem_map import COLUMNS

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
COAL, GAS = "Conventional Steam Coal", "Natural Gas Steam Turbine"


@pytest.fixture
def mod(monkeypatch):
    monkeypatch.syspath_prepend(str(SCRIPTS))
    spec = importlib.util.spec_from_file_location(
        "verify_crosswalk", SCRIPTS / "verify_crosswalk.py"
    )
    m = importlib.util.module_from_spec(spec)
    sys.modules["verify_crosswalk"] = m
    spec.loader.exec_module(m)
    m.fail.count = 0
    monkeypatch.setattr(
        conversion_boundaries, "load_decisions", dict
    )  # no stale decisions unless a test adds one
    return m


@pytest.fixture
def engine(pg_url):
    e = create_engine(pg_url)
    with e.begin() as conn:
        conn.execute(text(
            "CREATE TABLE mv_eia_unit_monthly (plant_code text, generator_id text, month timestamptz, "
            "timestamp_ms bigint, net_generation_mwh double precision)"
        ))  # fmt: skip
        conn.execute(text(
            "CREATE TABLE mv_occto_unit_monthly (plant text, unit text, month timestamptz, fuel_type text, "
            "generation_mwh double precision)"
        ))  # fmt: skip
        conn.execute(text(
            "INSERT INTO mv_eia_unit_monthly VALUES ('3149','1','2024-01-01Z',1704067200000,90), "
            "('3149','1','2024-02-01Z',1706745600000,80), ('8066','1','2024-02-01Z',1706745600000,70)"
        ))  # fmt: skip
        conn.execute(text(
            "INSERT INTO mv_occto_unit_monthly VALUES ('鈴川エネルギーセンター発電所','単独','2024-03-01Z','coal',30)"
        ))  # fmt: skip
    yield e
    e.dispose()


def _map(rows):
    return pd.DataFrame(rows, columns=COLUMNS + BOUNDARY_COLUMNS)


ROWS = [
    ("EIA", "3149", "1", "L_M", "G100000107004", "G100000107004", "gem-eia-id", COAL, True, None,
     "G_M1_GAS", "2024-01", None, "reviewed", COAL, GAS),
    ("EIA", "8066", "1", "L_B", "G100000105177", "G100000105177", "gem-eia-id", GAS, False, None,
     "G_B1_GAS", "2023-12", None, "reviewed", COAL, GAS),
    ("OCCTO", "鈴川エネルギーセンター発電所", "単独", "L_S", "G100000109766", "G100000109766", "single-unit", "coal",
     True, None, "G_S_BIO", "2022-12", None, "gem-year-end", "coal", "biomass"),
]  # fmt: skip
UNITS = pd.DataFrame(
    [("G100000107004", "L_M", "G_M1_GAS"), ("G_M1_GAS", "L_M", None), ("G100000105177", "L_B", "G_B1_GAS"),
     ("G_B1_GAS", "L_B", None), ("G100000109766", "L_S", "G_S_BIO"), ("G_S_BIO", "L_S", None)],
    columns=["gem_unit_id", "gem_location_id", "conversion_to_id"],
)  # fmt: skip


def test_a_correct_split_passes(mod, engine, capsys):
    mod.conversion_gate(engine, _map(ROWS), UNITS)
    assert mod.fail.count == 0
    assert "ok    G-CONVERSION" in capsys.readouterr().out


def test_a_missing_required_boundary_fails(mod, engine, capsys):
    mod.conversion_gate(
        engine,
        _map(ROWS[:2] + [(*ROWS[2][:10], None, None, None, None, None, None)]),
        UNITS,
    )
    assert mod.fail.count == 1
    assert (
        "OCCTO G100000109766 should have a gem-year-end boundary"
        in capsys.readouterr().out
    )


def test_a_source_the_build_lacks_is_not_required(mod, engine):
    mod.conversion_gate(
        engine, _map(ROWS[2:]), UNITS
    )  # OCCTO only, e.g. --sources OCCTO
    assert mod.fail.count == 0


def test_gem_units_without_conversion_data_fail(mod, engine, capsys):
    mod.conversion_gate(engine, _map(ROWS), UNITS.drop(columns=["conversion_to_id"]))
    assert "run scripts/fetch_gem.py --force-detail" in capsys.readouterr().out


def test_an_unresolved_reviewed_month_fails(mod, engine, capsys):
    rows = [*ROWS, ("EIA", "2", "1", "L_U", "G_U", "G_U", "gem-eia-id", COAL, True, None,
                    None, "2023-01", None, "unresolved", COAL, None)]  # fmt: skip
    mod.conversion_gate(engine, _map(rows), UNITS)
    assert "1 reviewed months without a GEM twin" in capsys.readouterr().out


def test_a_decision_naming_a_unit_the_map_lacks_fails(mod, engine, capsys, monkeypatch):
    ghost = BoundaryDecision(
        source_system="EIA", plant_key="9", unit_key="9", treatment="keep_as_is",
        reason="r", decided_by="N. Abad", decided_on="2026-11-21", ref="tracker#10",
    )  # fmt: skip
    monkeypatch.setattr(
        conversion_boundaries, "load_decisions", lambda: {ghost.key: ghost}
    )
    mod.conversion_gate(engine, _map(ROWS), UNITS)
    assert "names units the map lacks: EIA 9 9" in capsys.readouterr().out


def test_a_unit_month_counted_twice_fails(mod, engine, capsys):
    mod.conversion_gate(engine, _map([*ROWS, ROWS[0]]), UNITS)
    assert "source unit-months appear twice" in capsys.readouterr().out


def test_a_twin_at_another_gem_plant_fails(mod, engine, capsys):
    units = UNITS.copy()
    units.loc[units["gem_unit_id"] == "G_M1_GAS", "gem_location_id"] = "L_ELSEWHERE"
    mod.conversion_gate(engine, _map(ROWS), units)
    assert "conversion twins not at their unit's GEM plant" in capsys.readouterr().out


def test_view_sql_that_ignores_the_boundaries_fails(mod, engine, capsys, monkeypatch):
    # main's branch SQL, before the split: coal after the boundary, Suzukawa's biomass months as coal.
    import bootstrap_neon_db as boot

    old = " UNION ALL ".join([
        "SELECT m.source_system, m.gem_location_id, m.gem_unit_id, m.gem_unit_ids, m.plant_key, m.unit_key, u.month, "
        "u.timestamp_ms, u.net_generation_mwh AS generation_mwh, m.fuel, m.is_coal, m.map_source "
        "FROM mv_eia_unit_monthly u JOIN unit_gem_map m ON m.source_system = 'EIA' AND m.plant_key = u.plant_code "
        "AND m.unit_key = u.generator_id",
        "SELECT m.source_system, m.gem_location_id, m.gem_unit_id, m.gem_unit_ids, m.plant_key, m.unit_key, u.month, "
        "0::bigint, u.generation_mwh, u.fuel_type, TRUE, m.map_source FROM mv_occto_unit_monthly u "
        "JOIN unit_gem_map m ON m.source_system = 'OCCTO' AND m.plant_key = u.plant AND m.unit_key = u.unit "
        "WHERE u.fuel_type = 'coal'",
    ])  # fmt: skip
    monkeypatch.setattr(boot, "gem_unit_rows_sql", lambda engine: old)
    mod.conversion_gate(engine, _map(ROWS), UNITS)
    assert (
        "count as coal after their boundary or in a non-coal month"
        in capsys.readouterr().out
    )

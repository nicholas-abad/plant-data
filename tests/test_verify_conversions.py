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

from src.conversion_boundaries import BOUNDARY_COLUMNS
from src.unit_gem_map import COLUMNS

MONTOUR = ("EIA", "3149", "1")
BRIDGER = ("EIA", "8066", "1")
SUZUKAWA = ("OCCTO", "鈴川エネルギーセンター発電所", "単独")


@pytest.fixture
def mod(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / "scripts"))
    spec = importlib.util.spec_from_file_location(
        "verify_crosswalk",
        Path(__file__).resolve().parents[1] / "scripts" / "verify_crosswalk.py",
    )
    m = importlib.util.module_from_spec(spec)
    sys.modules["verify_crosswalk"] = m
    spec.loader.exec_module(m)
    m.fail.count = 0
    return m


@pytest.fixture
def engine(pg_url):
    e = create_engine(pg_url)
    with e.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE mv_eia_unit_monthly (plant_code text, generator_id text, month timestamptz, "
                "timestamp_ms bigint, net_generation_mwh double precision)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE mv_occto_unit_monthly (plant text, unit text, month timestamptz, fuel_type text, "
                "generation_mwh double precision)"
            )
        )
        conn.execute(
            text(
                "INSERT INTO mv_eia_unit_monthly VALUES ('3149','1','2024-01-01Z',1704067200000,90), "
                "('3149','1','2024-02-01Z',1706745600000,80), ('8066','1','2024-02-01Z',1706745600000,70)"
            )
        )
        conn.execute(
            text(
                "INSERT INTO mv_occto_unit_monthly VALUES "
                "('鈴川エネルギーセンター発電所','単独','2024-03-01Z','coal',30)"
            )
        )
    yield e
    e.dispose()


def _map(rows):
    return pd.DataFrame(rows, columns=COLUMNS + BOUNDARY_COLUMNS)


ROWS = [
    (*MONTOUR, "L_M", "G_M1", "G_M1", "gem-eia-id", "Conventional Steam Coal", True, None,
     "G_M1_GAS", "2024-01", "reviewed", "Conventional Steam Coal", "Natural Gas Steam Turbine"),
    (*BRIDGER, "L_B", "G_B1", "G_B1", "gem-eia-id", "Natural Gas Steam Turbine", False, None,
     "G_B1_GAS", "2023-12", "reviewed", "Conventional Steam Coal", "Natural Gas Steam Turbine"),
    (*SUZUKAWA, "L_S", "G_S", "G_S", "single-unit", "coal", True, None,
     "G_S_BIO", "2022-12", "gem-year-end", "coal", "biomass"),
]  # fmt: skip
UNITS = pd.DataFrame(
    [("G_M1", "L_M", "G_M1_GAS"), ("G_M1_GAS", "L_M", None), ("G_B1", "L_B", "G_B1_GAS"),
     ("G_B1_GAS", "L_B", None), ("G_S", "L_S", "G_S_BIO"), ("G_S_BIO", "L_S", None)],
    columns=["gem_unit_id", "gem_location_id", "conversion_to_id"],
)  # fmt: skip


def test_a_correct_split_passes(mod, engine, capsys):
    mod.conversion_gate(engine, _map(ROWS), UNITS)
    assert mod.fail.count == 0
    assert "ok    G-CONVERSION" in capsys.readouterr().out


def test_a_missing_required_boundary_fails(mod, engine, capsys):
    mod.conversion_gate(engine, _map(ROWS[:2]), UNITS)
    assert mod.fail.count == 1
    assert (
        "鈴川エネルギーセンター発電所 単独 should have a gem-year-end boundary"
        in capsys.readouterr().out
    )


def test_gem_units_without_conversion_data_fail(mod, engine, capsys):
    mod.conversion_gate(engine, _map(ROWS), UNITS.drop(columns=["conversion_to_id"]))
    assert "run scripts/fetch_gem.py --force-detail" in capsys.readouterr().out


def test_a_unit_month_counted_twice_fails(mod, engine, capsys):
    mod.conversion_gate(engine, _map([*ROWS, ROWS[0]]), UNITS)
    assert "source unit-months appear twice" in capsys.readouterr().out


def test_a_twin_at_another_gem_plant_fails(mod, engine, capsys):
    units = UNITS.copy()
    units.loc[units["gem_unit_id"] == "G_M1_GAS", "gem_location_id"] = "L_ELSEWHERE"
    mod.conversion_gate(engine, _map(ROWS), units)
    assert "conversion twins not at their unit's GEM plant" in capsys.readouterr().out

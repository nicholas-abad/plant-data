"""G-UNITMAP's US coverage: an empty register join fails instead of passing as NaN,
and a generator without a Form 860 technology fails instead of counting as "not coal"."""

import datetime as dt
import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from src import eia_unit_map
from src.unit_gem_map import COLUMNS

COAL = "Conventional Steam Coal"
REGISTER_YEAR_END = dt.date(2024, 12, 31)


@pytest.fixture
def mod():
    spec = importlib.util.spec_from_file_location(
        "verify_crosswalk",
        Path(__file__).resolve().parents[1] / "scripts" / "verify_crosswalk.py",
    )
    m = importlib.util.module_from_spec(spec)
    sys.modules["verify_crosswalk"] = m
    spec.loader.exec_module(m)
    m.fail.count = 0
    return m


def test_no_us_coal_generation_in_the_register_join_fails(mod, monkeypatch, tmp_path):
    umap = pd.DataFrame(
        [("EIA", "6166", "1", "L_RP", "G_RP1", "G_RP1", "gem-eia-id", "Conventional Steam Coal", True, None)],
        columns=COLUMNS,
    )
    path = tmp_path / "unit_gem_map.parquet"
    umap.to_parquet(path)
    monkeypatch.setattr(mod, "UNIT_MAP_FILE", path)
    monkeypatch.setattr(mod, "_make_engine", MagicMock)
    monkeypatch.setattr(mod, "unit_overlaps", lambda engine, umap: "")
    monkeypatch.setattr(mod, "no_fuel_gate", lambda engine, eia: None)
    # mv_eia_unit_monthly joined to an unkeyed (or empty) eia_generator_info: no rows
    monkeypatch.setattr(mod.pd, "read_sql", lambda *a, **k: pd.DataFrame(columns=["plant_key", "unit_key", "g"]))
    xw = pd.DataFrame(columns=["source_system", "plant_code", "gem_location_id"])
    units = pd.DataFrame([("G_RP1", "L_RP")], columns=["gem_unit_id", "gem_location_id"])

    mod.unit_map_gate(xw, units)

    assert mod.fail.count == 1


def test_the_old_india_loader_fails_instead_of_skipping(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / "scripts"))
    from bootstrap_neon_db import load_npp_unit_map

    with pytest.raises(RuntimeError, match="load_unit_gem_map"):
        load_npp_unit_map(engine=None)


# --- the no-fuel guard ---------------------------------------------------------


def _eia_rows(*rows):
    """unit_gem_map EIA rows as (plant_key, unit_key, fuel)."""
    return pd.DataFrame(
        [("EIA", p, u, "L", None, None, "crosswalk-plant", fuel, fuel == COAL, None) for p, u, fuel in rows],
        columns=COLUMNS,
    )


def test_an_old_generator_without_a_fuel_fails_and_a_new_one_only_warns():
    """Zimmer ran 2019-2022 and had no register row: it counted as "not coal" (93.6 TWh with 45 others)."""
    rows = _eia_rows(("6019", "ST1", None), ("645", "CT5", None), ("3122", "1", COAL))
    first = {("6019", "ST1"): dt.date(2019, 1, 1), ("645", "CT5"): dt.date(2025, 9, 1), ("3122", "1"): dt.date(2019, 1, 1)}
    found = eia_unit_map.no_fuel_findings(rows, first, set(), REGISTER_YEAR_END)
    assert found.failing == ["6019/ST1 (first generation 2019-01)"]
    assert found.new == ["645/CT5 (first generation 2025-09)"]
    assert (found.skipped, found.stale) == ([], [])


def test_the_register_year_end_is_the_last_month_that_must_have_a_fuel():
    rows = _eia_rows(("1", "A", None), ("1", "B", None))
    first = {("1", "A"): dt.date(2024, 12, 1), ("1", "B"): dt.date(2025, 1, 1)}
    found = eia_unit_map.no_fuel_findings(rows, first, set(), REGISTER_YEAR_END)
    assert (found.failing, found.new) == (["1/A (first generation 2024-12)"], ["1/B (first generation 2025-01)"])


def test_a_generator_without_a_first_month_is_not_excused_as_new():
    found = eia_unit_map.no_fuel_findings(_eia_rows(("1", "A", None)), {}, set(), REGISTER_YEAR_END)
    assert (found.failing, found.new) == (["1/A"], [])


def test_a_listed_generator_is_skipped_and_a_newly_mapped_unlisted_one_fails():
    """Linking a plant to GEM adds fallback rows for its generators; only listed ones may lack a fuel."""
    rows = _eia_rows(("62192", "IS", None), ("63479", "1", None))
    first = {("62192", "IS"): dt.date(2022, 6, 1), ("63479", "1"): dt.date(2023, 3, 1)}
    found = eia_unit_map.no_fuel_findings(rows, first, {("62192", "IS")}, REGISTER_YEAR_END)
    assert found.skipped == ["62192/IS (first generation 2022-06)"]
    assert found.failing == ["63479/1 (first generation 2023-03)"]


def test_a_listed_generator_that_is_not_in_the_map_is_silent_and_one_with_a_fuel_is_stale():
    rows = _eia_rows(("1571", "ST1", COAL))
    listed = {("1571", "ST1"), ("302", "ST1")}  # Encina's plant is not linked to GEM: no row at all
    found = eia_unit_map.no_fuel_findings(rows, {("1571", "ST1"): dt.date(2019, 1, 1)}, listed, REGISTER_YEAR_END)
    assert found.stale == ["1571/ST1 (first generation 2019-01)"]
    assert (found.failing, found.new, found.skipped) == ([], [], [])


def test_the_committed_exceptions_name_their_generators_once_with_a_reason():
    listed = eia_unit_map.load_no_fuel_exceptions()
    assert len(listed) == 18
    assert {("62192", "IS"), ("1571", "ST1"), ("55641", "CTG3"), ("302", "5")} <= listed


def test_an_exception_without_a_reason_is_refused(tmp_path):
    path = tmp_path / "exceptions.csv"
    path.write_text("plant_code,generator_id,plant_name,form_860_technology,reason\n1,A,Some Plant,,\n")
    with pytest.raises(ValueError, match="lack a plant_code, generator_id or reason"):
        eia_unit_map.load_no_fuel_exceptions(path)


def _register_database(pg_url, with_edition: bool = True):
    """mv_eia_unit_monthly and eia_generator_info as the gate reads them."""
    engine = create_engine(pg_url)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE mv_eia_unit_monthly (plant_code varchar, generator_id varchar, month timestamptz)"))
        conn.execute(text(
            "INSERT INTO mv_eia_unit_monthly VALUES ('6019', 'ST1', '2019-01-01'), ('6019', 'ST1', '2022-07-01'), "
            "('645', 'CT5', '2025-09-01'), ('62192', 'IS', '2022-06-01'), ('3122', '1', '2019-01-01')"
        ))
        edition = ", source_edition bigint" if with_edition else ""
        conn.execute(text(f"CREATE TABLE eia_generator_info (plant_code text, generator_id text, technology text{edition})"))
        conn.execute(text(
            f"INSERT INTO eia_generator_info VALUES ('3122', '1', '{COAL}'{', 2024' if with_edition else ''})"
        ))
    return engine


def test_the_gate_fails_on_an_old_generator_without_a_fuel(mod, pg_url, capsys):
    engine = _register_database(pg_url)
    try:
        mod.no_fuel_gate(engine, _eia_rows(("6019", "ST1", None), ("645", "CT5", None), ("62192", "IS", None)))
    finally:
        engine.dispose()
    out = capsys.readouterr().out
    # verify_crosswalk.main() exits 1 on any counted failure, which stops the weekly load step.
    assert mod.fail.count == 1
    assert "FAIL  G-UNITMAP: 1 US generators with generation by 2024-12-31 have no Form 860 technology" in out
    assert "6019/ST1 (first generation 2019-01)" in out
    assert "warn  G-UNITMAP: 1 US generators started after the Form 860 2024 register" in out
    assert "warn  G-UNITMAP: 1 US generators without a Form 860 row are skipped" in out


def test_the_gate_passes_when_only_new_and_listed_generators_lack_a_fuel(mod, pg_url, capsys):
    """A generator EIA first reports next year must not stop the weekly crosswalk for every source."""
    engine = _register_database(pg_url)
    try:
        mod.no_fuel_gate(engine, _eia_rows(("645", "CT5", None), ("62192", "IS", None), ("3122", "1", COAL)))
    finally:
        engine.dispose()
    assert mod.fail.count == 0
    assert "ok    G-UNITMAP: every US generator in the map has a Form 860 technology" in capsys.readouterr().out


def test_the_gate_fails_on_a_register_loaded_before_the_editions(mod, pg_url, capsys):
    """Merged before the production load: say what to run instead of guessing the register's year."""
    engine = _register_database(pg_url, with_edition=False)
    try:
        mod.no_fuel_gate(engine, _eia_rows(("3122", "1", COAL)))
    finally:
        engine.dispose()
    assert mod.fail.count == 1
    assert "no source_edition column" in capsys.readouterr().out


def test_the_unit_map_gate_runs_the_no_fuel_guard(mod, monkeypatch, tmp_path):
    """A map whose only fault is a US generator without a fuel fails G-UNITMAP."""
    umap = pd.DataFrame(
        [
            ("EIA", "6166", "1", "L_RP", "G_RP1", "G_RP1", "gem-eia-id", COAL, True, None),
            ("EIA", "6019", "ST1", "L_ZI", "G_ZI1", "G_ZI1", "gem-eia-id", None, False, None),
        ],
        columns=COLUMNS,
    )
    path = tmp_path / "unit_gem_map.parquet"
    umap.to_parquet(path)
    monkeypatch.setattr(mod, "UNIT_MAP_FILE", path)
    monkeypatch.setattr(mod, "_make_engine", MagicMock)
    monkeypatch.setattr(mod, "unit_overlaps", lambda engine, umap: "")
    seen = []
    monkeypatch.setattr(mod, "no_fuel_gate", lambda engine, eia: seen.append(eia) or mod.fail("G-UNITMAP: no fuel"))
    monkeypatch.setattr(
        mod.pd, "read_sql", lambda *a, **k: pd.DataFrame([("6166", "1", 5.0)], columns=["plant_key", "unit_key", "g"])
    )
    xw = pd.DataFrame(columns=["source_system", "plant_code", "gem_location_id"])
    units = pd.DataFrame([("G_RP1", "L_RP"), ("G_ZI1", "L_ZI")], columns=["gem_unit_id", "gem_location_id"])

    mod.unit_map_gate(xw, units)

    assert mod.fail.count == 1
    assert seen[0]["unit_key"].tolist() == ["1", "ST1"]

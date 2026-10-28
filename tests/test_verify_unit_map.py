"""G-UNITMAP's US coverage: an empty register join fails instead of passing as NaN."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from src.unit_gem_map import COLUMNS


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

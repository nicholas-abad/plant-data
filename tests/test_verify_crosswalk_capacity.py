"""gate7's fleet capacity: a GEM site's nameplate counts once, however many feed names link to it."""

import importlib.util
import sys
from pathlib import Path

import pandas as pd
import pytest


@pytest.fixture
def mod():
    spec = importlib.util.spec_from_file_location(
        "verify_crosswalk",
        Path(__file__).resolve().parents[1] / "scripts" / "verify_crosswalk.py",
    )
    m = importlib.util.module_from_spec(spec)
    sys.modules["verify_crosswalk"] = m
    spec.loader.exec_module(m)
    return m


def rows(*records):
    return pd.DataFrame(
        records,
        columns=["plant_name", "gem_location_id", "capacity_mw", "capacity_source"],
    )


def test_linking_a_renamed_plant_does_not_grow_the_fleet(mod):
    before = rows(
        ("Jorge Lacerda A", "L1", 857.0, "GEM"),
        ("Jorge Lacerda B", "L1", 857.0, "GEM"),
        ("U.JLACERDA A", None, None, None),
        ("Pampa Sul", "L2", 345.0, "GEM"),
    )
    after = rows(
        ("Jorge Lacerda A", "L1", 857.0, "GEM"),
        ("Jorge Lacerda B", "L1", 857.0, "GEM"),
        ("U.JLACERDA A", "L1", 857.0, "GEM"),
        ("Pampa Sul", "L2", 345.0, "GEM"),
    )
    assert mod.fleet_capacity_mw(before) == mod.fleet_capacity_mw(after) == 1202.0


def test_distinct_gem_capacities_at_one_site_both_count(mod):
    korba = rows(("KORBA-II", "L3", 200.0, "GEM"), ("KORBA-III", "L3", 240.0, "GEM"))
    assert mod.fleet_capacity_mw(korba) == 440.0


def test_unit_and_apportioned_capacities_are_summed(mod):
    units = rows(
        ("Neurath F", "L4", 1100.0, "ENTSOE_APPORTIONED"),
        ("Neurath G", "L4", 1100.0, "ENTSOE_APPORTIONED"),
        ("Hekinan 1", "L5", 700.0, "HJKS"),
        ("Hekinan 2", "L5", 700.0, "HJKS"),
    )
    assert mod.fleet_capacity_mw(units) == 3600.0

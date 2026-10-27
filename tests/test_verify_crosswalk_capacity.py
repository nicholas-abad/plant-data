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


def rows(source, *records):
    df = pd.DataFrame(
        records,
        columns=["plant_name", "gem_location_id", "capacity_mw", "capacity_source"],
    )
    return df.assign(source_system=source)


def test_linking_a_renamed_plant_does_not_grow_the_fleet(mod):
    before = rows(
        "ONS",
        ("Jorge Lacerda A", "L1", 857.0, "GEM"),
        ("Jorge Lacerda B", "L1", 857.0, "GEM"),
        ("U.JLACERDA A", None, None, None),
        ("Pampa Sul", "L2", 345.0, "GEM"),
    )
    after = rows(
        "ONS",
        ("Jorge Lacerda A", "L1", 857.0, "GEM"),
        ("Jorge Lacerda B", "L1", 857.0, "GEM"),
        ("U.JLACERDA A", "L1", 857.0, "GEM"),
        ("Pampa Sul", "L2", 345.0, "GEM"),
    )
    assert mod.fleet_capacity_mw(before) == mod.fleet_capacity_mw(after) == 1202.0


def test_npp_phases_with_the_same_nameplate_both_count(mod):
    chhabra = rows(
        "NPP",
        ("CHHABRA-I PH-1 TPP", "L3", 500.0, "GEM"),
        ("CHHABRA-I PH-2 TPP", "L3", 500.0, "GEM"),
    )
    assert mod.fleet_capacity_mw(chhabra) == 1000.0


def test_unit_and_apportioned_capacities_are_summed(mod):
    units = pd.concat(
        [
            rows(
                "ENTSOE",
                ("Neurath F", "L4", 1100.0, "ENTSOE_APPORTIONED"),
                ("Neurath G", "L4", 1100.0, "ENTSOE_APPORTIONED"),
            ),
            rows(
                "OCCTO",
                ("Hekinan 1", "L5", 700.0, "HJKS"),
                ("Hekinan 2", "L5", 700.0, "HJKS"),
            ),
        ]
    )
    assert mod.fleet_capacity_mw(units) == 3600.0

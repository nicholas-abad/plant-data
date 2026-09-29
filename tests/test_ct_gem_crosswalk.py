"""Tests for Climate TRACE's published CT → GEM links (no Neon; SQLite for the SQL)."""

import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from src.ct_gem_crosswalk import COLUMNS, CT_PLANT_UNITS_SQL, ct_gem_links_frame

COMMITTED = (
    Path(__file__).resolve().parent.parent
    / "data"
    / "crosswalks"
    / "ct_gem_crosswalk.csv"
)


def _sheet(rows):
    return pd.DataFrame(
        rows,
        columns=[
            "original_inventory_sector",
            "native_source_id",
            "source_id",
            "gem_id",
        ],
    )


def test_keeps_power_rows_only_and_labels_kind():
    out = ct_gem_links_frame(
        _sheet(
            [
                ["electricity-generation", "PLDOLNA", "200", "G100000000002"],
                ["electricity-generation", "PLDOLNA", "200", "L100000000009"],
                ["cement", "XXCEM", "300", "P100000000001"],
            ]
        )
    )
    assert list(out.columns) == COLUMNS
    assert out.values.tolist() == [
        ["200", "PLDOLNA", "G100000000002", "unit"],
        ["200", "PLDOLNA", "L100000000009", "location"],
    ]


def test_committed_csv_round_trips():
    committed = pd.read_csv(COMMITTED, dtype=str, keep_default_na=False, na_values=[""])
    out = ct_gem_links_frame(committed)
    # The committed file IS canonical output: same rows, order and values.
    assert out.values.tolist() == committed[COLUMNS].values.tolist()
    assert set(out["gem_id_kind"]) == {"unit", "location"}


@pytest.mark.parametrize(
    "row, message",
    [
        (["electricity-generation", "X", "200", "P100000000001"], "neither a unit"),
        (["electricity-generation", "X", "20a", "G100000000001"], "non-numeric"),
        (["electricity-generation", "X", None, "G100000000001"], "blank"),
        (["electricity-generation", "X", "   ", "G100000000001"], "blank"),
    ],
)
def test_rejects_rows_a_reader_could_misjoin(row, message):
    with pytest.raises(ValueError, match=message):
        ct_gem_links_frame(_sheet([row]))


def test_rejects_duplicate_links():
    row = ["electricity-generation", "X", "200", "G100000000001"]
    with pytest.raises(ValueError, match="duplicate"):
        ct_gem_links_frame(_sheet([row, row]))


def test_rejects_a_sheet_with_no_power_rows():
    with pytest.raises(ValueError, match="no electricity-generation rows"):
        ct_gem_links_frame(_sheet([["cement", "X", "300", "P1"]]))


def _plant_units(links, units):
    """Run CT_PLANT_UNITS_SQL as written against an in-memory database."""
    db = sqlite3.connect(":memory:")
    db.execute(
        "CREATE TABLE ct_gem_crosswalk (climatetrace_id, ct_native_source_id, gem_id, gem_id_kind)"
    )
    db.execute("CREATE TABLE gem_units (gem_unit_id, gem_location_id)")
    db.executemany("INSERT INTO ct_gem_crosswalk VALUES (?, ?, ?, ?)", links)
    db.executemany("INSERT INTO gem_units VALUES (?, ?)", units)
    return sorted(db.execute(CT_PLANT_UNITS_SQL).fetchall())


UNITS = [
    ("G1", "L1"),
    ("G2", "L1"),
    ("GGAS", "L1"),  # a coal+gas site
    ("G3", "L2"),
    ("G4", "L2"),  # a second phase of the complex
    ("G5", "L3"),
    ("G6", "L3"),  # a site shared with another plant
]


def test_named_units_and_their_own_location_count_once():
    # The usual shape: units named, plus their location repeated — no double count.
    links = [
        ("1", "A", "G1", "unit"),
        ("1", "A", "GGAS", "unit"),
        ("1", "A", "L1", "location"),
    ]
    assert _plant_units(links, UNITS) == [("1", "G1"), ("1", "G2"), ("1", "GGAS")]


def test_a_named_unit_brings_its_whole_site():
    # The sheet names only the coal unit; the station's gas unit counts too.
    assert _plant_units([("1", "A", "G1", "unit")], UNITS) == [
        ("1", "G1"),
        ("1", "G2"),
        ("1", "GGAS"),
    ]


def test_every_location_of_a_complex_counts():
    links = [("1", "A", "G1", "unit"), ("1", "A", "L2", "location")]
    assert _plant_units(links, UNITS) == [
        ("1", "G1"),
        ("1", "G2"),
        ("1", "G3"),
        ("1", "G4"),
        ("1", "GGAS"),
    ]


def test_a_shared_location_counts_for_each_plant():
    # Per-plant capacity is whole-site for both; totals must not add them.
    links = [("1", "A", "G5", "unit"), ("2", "B", "L3", "location")]
    assert _plant_units(links, UNITS) == [
        ("1", "G5"),
        ("1", "G6"),
        ("2", "G5"),
        ("2", "G6"),
    ]


def test_ids_gem_does_not_carry_are_dropped():
    links = [("1", "A", "G999", "unit"), ("1", "A", "L999", "location")]
    assert _plant_units(links, UNITS) == []

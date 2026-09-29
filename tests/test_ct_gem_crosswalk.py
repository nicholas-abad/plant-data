"""Tests for Climate TRACE's published CT → GEM links (no database)."""

from pathlib import Path

import pandas as pd
import pytest

from src.ct_gem_crosswalk import COLUMNS, ct_gem_links_frame

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
    assert len(out) == len(committed)
    assert set(out["gem_id_kind"]) == {"unit", "location"}


@pytest.mark.parametrize(
    "row, message",
    [
        (["electricity-generation", "X", "200", "P100000000001"], "neither a unit"),
        (["electricity-generation", "X", "20a", "G100000000001"], "non-numeric"),
        (["electricity-generation", "X", None, "G100000000001"], "blank"),
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

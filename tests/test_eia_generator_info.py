"""Tests for the EIA Form 860 generator reference frame (no database)."""

from pathlib import Path

import pandas as pd
import pytest

from src.eia_generator_info import (
    OPERABLE_SHEET,
    RETIRED_SHEET,
    eia_generator_info_frame,
)

REAL_FILE = (
    Path(__file__).resolve().parent.parent
    / "data"
    / "crosswalks"
    / "3_1_Generator_Y2024.xlsx"
)
HEADER = [
    "Plant Code",
    "Generator ID",
    "Technology",
    "Prime Mover",
    "Energy Source 1",
    "Nameplate Capacity (MW)",
    "Status",
]
COAL = "Conventional Steam Coal"


def _workbook(tmp_path, operable, retired):
    """A Form 860-shaped workbook: a title row above each sheet's header."""
    path = tmp_path / "generators.xlsx"
    with pd.ExcelWriter(path) as xl:
        for sheet, rows in ((OPERABLE_SHEET, operable), (RETIRED_SHEET, retired)):
            pd.DataFrame([["Form 860 title row"] + [None] * 6, HEADER, *rows]).to_excel(
                xl, sheet_name=sheet, header=False, index=False
            )
    return path


def test_retired_generators_join_the_operable_ones(tmp_path):
    path = _workbook(
        tmp_path,
        operable=[[3122, "1", COAL, "ST", "BIT", 660.0, "OP"]],
        retired=[
            [3122, "2", COAL, "ST", "BIT", 660.0, "RE"],
            [9999, "A", COAL, "ST", "SUB", 500.0, "CN"],  # never built
            [9998, "B", COAL, "ST", "SUB", 400.0, "IP"],  # never built
        ],
    )
    df = eia_generator_info_frame(path)
    assert df[["plant_code", "generator_id", "status"]].values.tolist() == [
        ["3122", "1", "OP"],
        ["3122", "2", "RE"],
    ]


def test_keys_are_strings_like_the_generation_tables(tmp_path):
    path = _workbook(tmp_path, [[6137, 1, COAL, "ST", "BIT", 265.0, "OP"]], [])
    row = eia_generator_info_frame(path).iloc[0]
    assert (row.plant_code, row.generator_id) == ("6137", "1")


def test_a_generator_on_both_sheets_fails_loudly(tmp_path):
    row = [3122, "1", COAL, "ST", "BIT", 660.0]
    path = _workbook(tmp_path, [row + ["OP"]], [row + ["RE"]])
    with pytest.raises(ValueError, match="share a"):
        eia_generator_info_frame(path)


@pytest.mark.skipif(not REAL_FILE.exists(), reason="Form 860 workbook not downloaded")
def test_real_workbook_loads_with_retired_coal():
    df = eia_generator_info_frame(REAL_FILE)
    retired = df[df.status == "RE"]
    assert len(retired) > 4000
    # Homer City (3122) retired all its coal units in 2023.
    assert set(retired[retired.plant_code == "3122"].technology) == {COAL}

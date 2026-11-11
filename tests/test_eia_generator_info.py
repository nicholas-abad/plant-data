"""Tests for the EIA Form 860 generator reference frame (no database)."""

from pathlib import Path

import pandas as pd
import pytest

from src import eia_generator_info as register
from src.eia_generator_info import (
    OPERABLE_SHEET,
    RETIRED_SHEET,
    eia_generator_info_frame,
    eia_generator_key,
    read_supplement,
    retired_rows,
    retired_supplement,
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
GAS = "Natural Gas Steam Turbine"
WOOD = "Wood/Wood Waste Biomass"
EDITIONS_DIR = REAL_FILE.parent.parent / "eia860"


def _workbook(tmp_path, operable, retired, name="generators.xlsx"):
    """A Form 860-shaped workbook: a title row above each sheet's header."""
    path = tmp_path / name
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
    df = eia_generator_info_frame(path, None)
    assert df[["plant_code", "generator_id", "status"]].values.tolist() == [
        ["3122", "1", "OP"],
        ["3122", "2", "RE"],
    ]


def test_keys_are_strings_like_the_generation_tables(tmp_path):
    path = _workbook(tmp_path, [[6137, 1, COAL, "ST", "BIT", 265.0, "OP"]], [])
    row = eia_generator_info_frame(path, None).iloc[0]
    assert (row.plant_code, row.generator_id) == ("6137", "1")


def test_a_generator_on_both_sheets_fails_loudly(tmp_path):
    row = [3122, "1", COAL, "ST", "BIT", 660.0]
    path = _workbook(tmp_path, [row + ["OP"]], [row + ["RE"]])
    with pytest.raises(ValueError, match="share a"):
        eia_generator_info_frame(path, None)


@pytest.mark.skipif(not REAL_FILE.exists(), reason="Form 860 workbook not downloaded")
def test_real_workbook_loads_with_retired_coal():
    df = eia_generator_info_frame(REAL_FILE, None)
    retired = df[df.status == "RE"]
    assert len(retired) > 4000
    # Homer City (3122) retired all its coal units in 2023.
    assert set(retired[retired.plant_code == "3122"].technology) == {COAL}


# --- several editions --------------------------------------------------------


def _retired(tmp_path, edition, rows):
    """retired_rows() of a one-edition workbook whose retired sheet holds ``rows``."""
    return retired_rows(_workbook(tmp_path, [], rows, name=f"y{edition}.xlsx"), edition)


def _supplement_csv(tmp_path, older, base_path):
    path = tmp_path / "supplement.csv"
    retired_supplement(older, register.base_frame(base_path)).rows.to_csv(path, index=False)
    return path


def test_generators_retired_in_older_editions_join_the_base_edition(tmp_path):
    """W H Zimmer retired in 2022; the 2024 edition lists it on neither sheet."""
    base = _workbook(
        tmp_path,
        operable=[[3122, "1", COAL, "ST", "BIT", 660.0, "OP"]],
        retired=[[1702, "1", COAL, "ST", "SUB", 272.0, "RE"]],  # Dan E Karn, retired 2023
    )
    older = {
        2022: _retired(tmp_path, 2022, [[6019, "ST1", COAL, "ST", "RC", 1425.6, "RE"]]),
        2019: _retired(tmp_path, 2019, [[10384, "GEN1", COAL, "ST", "BIT", 57.8, "RE"]]),
    }
    df = eia_generator_info_frame(base, _supplement_csv(tmp_path, older, base))
    assert df[["plant_code", "generator_id", "status", "source_edition"]].values.tolist() == [
        ["3122", "1", "OP", 2024],
        ["1702", "1", "RE", 2024],
        ["6019", "ST1", "RE", 2022],
        ["10384", "GEN1", "RE", 2019],
    ]
    assert set(df["technology"]) == {COAL}


def test_the_newest_edition_listing_a_generator_wins(tmp_path):
    base = _workbook(tmp_path, [[1, "X", GAS, "ST", "NG", 1.0, "OP"]], [])
    older = {
        2019: _retired(tmp_path, 2019, [[6019, "ST1", COAL, "ST", "BIT", 1300.0, "RE"]]),
        2022: _retired(tmp_path, 2022, [[6019, "ST1", COAL, "ST", "RC", 1425.6, "RE"]]),
        2020: _retired(tmp_path, 2020, [[6019, "ST1", COAL, "ST", "BIT", 1300.0, "RE"]]),
    }
    rows = retired_supplement(older, register.base_frame(base)).rows
    assert rows[["energy_source_1", "nameplate_capacity_mw", "source_edition"]].values.tolist() == [
        ["RC", 1425.6, 2022]
    ]


def test_an_operable_generator_is_never_overridden_by_an_older_retired_row(tmp_path):
    """24 generators are 'RE' in an older edition and Operable again in 2024."""
    base = _workbook(tmp_path, [[3122, "1", COAL, "ST", "BIT", 660.0, "OP"]], [])
    older = {2021: _retired(tmp_path, 2021, [[3122, "01", GAS, "ST", "NG", 660.0, "RE"]])}
    supplement = retired_supplement(older, register.base_frame(base))
    assert supplement.rows.empty
    df = eia_generator_info_frame(base, _supplement_csv(tmp_path, older, base))
    assert df[["generator_id", "technology", "status", "source_edition"]].values.tolist() == [
        ["1", COAL, "OP", 2024]
    ]
    # Reported, not applied: the base edition says coal, the older retired row says gas (as plant 165).
    assert supplement.technology_changes[["plant_code", "kept"]].values.tolist() == [
        ["3122", f"base edition: {COAL}"]
    ]


def test_a_supplement_row_the_base_edition_has_is_dropped_at_load(tmp_path):
    """A supplement built against an older base must not add a second row once the base lists the unit."""
    base = _workbook(tmp_path, [], [[6019, "ST1", COAL, "ST", "RC", 1425.6, "RE"]])
    csv = tmp_path / "supplement.csv"
    pd.DataFrame(
        [["6019", "st 1", GAS, "ST", "NG", 1.0, "RE", 2022]], columns=register.TABLE_COLUMNS
    ).to_csv(csv, index=False)
    df = eia_generator_info_frame(base, csv)
    assert df[["generator_id", "technology", "source_edition"]].values.tolist() == [["ST1", COAL, 2024]]


def test_coal_in_one_edition_and_not_in_another_fails_loudly(tmp_path):
    base = _workbook(tmp_path, [[1, "X", GAS, "ST", "NG", 1.0, "OP"]], [])
    older = {
        2019: _retired(tmp_path, 2019, [[645, "ST1", COAL, "ST", "BIT", 445.5, "RE"]]),
        2021: _retired(tmp_path, 2021, [[645, "ST1", GAS, "ST", "NG", 445.5, "RE"]]),
    }
    with pytest.raises(ValueError, match="coal in one Form 860 edition and not in another.*645/ST1"):
        retired_supplement(older, register.base_frame(base))


def test_another_technology_change_is_reported_and_the_newest_wins(tmp_path):
    base = _workbook(tmp_path, [[1, "X", GAS, "ST", "NG", 1.0, "OP"]], [])
    older = {
        2019: _retired(tmp_path, 2019, [[7737, "1", GAS, "ST", "NG", 80.0, "RE"]]),
        2023: _retired(tmp_path, 2023, [[7737, "1", WOOD, "ST", "WDS", 80.0, "RE"]]),
    }
    supplement = retired_supplement(older, register.base_frame(base))
    assert supplement.rows["technology"].tolist() == [WOOD]
    assert supplement.technology_changes.values.tolist() == [
        ["7737", "1", f"2019: {GAS}; 2023: {WOOD}", "2023 (newest)"]
    ]


@pytest.mark.parametrize(
    ("plant_code", "generator_id", "key"),
    [
        ("56298", "01", "1"),  # Dry Fork: Form 923 says 1, Form 860 says 01
        ("1", "1", "1"),
        ("1", "01A", "1A"),
        ("1", "000", "0"),
        ("1", "st 1", "ST1"),
        ("6166", "MB1", "1"),  # Rockport's MB1/MB2 are its units 1/2
        ("6166", "mb2", "2"),
        ("6166", "MB3", "MB3"),
        ("1", "MB1", "MB1"),
        ("56032", "0001", "0001"),  # two turbines here: 0001 and 001
        ("56032", "001", "001"),
        ("1", "6A/B", "6A/B"),
    ],
)
def test_generator_key_spells_an_id_one_way(plant_code, generator_id, key):
    assert eia_generator_key(plant_code, generator_id) == key


def test_two_spellings_of_one_generator_across_editions_are_one_generator(tmp_path):
    """Keyed, "1" (base) and "01" (older) are the same unit: the older row must not be added."""
    base = _workbook(tmp_path, [], [[56298, "01", COAL, "ST", "SUB", 483.7, "RE"]])
    older = {2020: _retired(tmp_path, 2020, [[56298, "1", COAL, "ST", "SUB", 483.7, "RE"]])}
    assert retired_supplement(older, register.base_frame(base)).rows.empty


def test_two_spellings_of_one_generator_in_one_edition_fail_loudly(tmp_path):
    """The swap's primary key is on the keyed ID; fail here with the rows named, not in SQL."""
    path = _workbook(
        tmp_path, [[3122, "1", COAL, "ST", "BIT", 660.0, "OP"], [3122, "01", COAL, "ST", "BIT", 660.0, "OP"]], []
    )
    with pytest.raises(ValueError, match="share a"):
        eia_generator_info_frame(path, None)


def test_plant_56032_keeps_0001_and_001_as_two_generators(tmp_path):
    path = _workbook(
        tmp_path, [[56032, "0001", GAS, "GT", "NG", 5.0, "OP"], [56032, "001", GAS, "GT", "NG", 5.0, "OP"]], []
    )
    assert eia_generator_info_frame(path, None)["generator_id"].tolist() == ["0001", "001"]


def test_a_supplement_row_with_an_operating_status_is_refused(tmp_path):
    """An older Operable row would count as current capacity (status IS DISTINCT FROM 'RE')."""
    csv = tmp_path / "supplement.csv"
    pd.DataFrame(
        [["1571", "ST1", COAL, "ST", "BIT", 364.0, "OP", 2020]], columns=register.TABLE_COLUMNS
    ).to_csv(csv, index=False)
    with pytest.raises(ValueError, match="not status 'RE'"):
        read_supplement(csv)


def test_supplement_ids_stay_text(tmp_path):
    csv = tmp_path / "supplement.csv"
    pd.DataFrame(
        [["1", "NA", GAS, "ST", "NG", 1.0, "RE", 2020], ["2", "01", GAS, "ST", "", 1.0, "RE", 2020]],
        columns=register.TABLE_COLUMNS,
    ).to_csv(csv, index=False)
    df = read_supplement(csv)
    assert df["generator_id"].tolist() == ["NA", "01"]
    assert df["energy_source_1"].isna().tolist() == [False, True]


# --- the committed supplement --------------------------------------------------


def test_committed_supplement_holds_retired_coal_the_base_edition_lost():
    rows = read_supplement(register.SUPPLEMENT_FILE)
    assert len(rows) == 877
    assert set(rows["status"]) == {"RE"} and set(rows["source_edition"]) == set(register.SUPPLEMENT_EDITIONS)
    zimmer = rows[(rows.plant_code == "6019") & (rows.generator_id == "ST1")].iloc[0]
    assert (zimmer.technology, zimmer.source_edition) == (COAL, 2022)
    sammis_coal = rows[(rows.plant_code == "2866") & (rows.technology == COAL)]
    assert sorted(sammis_coal.generator_id) == ["1", "2", "3", "4", "5", "6", "7"]


@pytest.mark.skipif(not REAL_FILE.exists(), reason="Form 860 workbook not downloaded")
def test_real_register_is_the_base_edition_plus_the_supplement():
    base = eia_generator_info_frame(REAL_FILE, None)
    df = eia_generator_info_frame(REAL_FILE, register.SUPPLEMENT_FILE)
    assert len(df) == len(base) + 877
    # Every base row is untouched, in place: nothing Operable changed, nothing replaced.
    pd.testing.assert_frame_equal(df.iloc[: len(base)], base)
    # Current capacity (status other than RE) is the base edition's.
    assert df.loc[df.status != "RE", "nameplate_capacity_mw"].sum() == base.loc[
        base.status != "RE", "nameplate_capacity_mw"
    ].sum()


@pytest.mark.skipif(
    # Every archive must be there: build() downloads a missing one, and a test must not.
    not (
        REAL_FILE.exists()
        and all((EDITIONS_DIR / str(y) / f"eia860{y}.zip").exists() for y in register.SUPPLEMENT_EDITIONS)
    ),
    reason="Form 860 archives 2019-2023 or the 2024 workbook not downloaded (scripts/build_eia860_supplement.py)",
)
def test_committed_supplement_is_what_the_workbooks_give(monkeypatch):
    import json

    monkeypatch.syspath_prepend(str(REAL_FILE.parent.parent.parent / "scripts"))
    import build_eia860_supplement as build

    built = build.build()
    assert build.csv_text(built.supplement.rows) == register.SUPPLEMENT_FILE.read_text()
    assert built.manifest == json.loads(register.SUPPLEMENT_MANIFEST.read_text())


def test_a_leftover_workbook_is_replaced_by_the_one_in_the_archive(tmp_path, monkeypatch):
    """The manifest pins the zip; a different workbook beside it must not be what gets read."""
    import zipfile

    monkeypatch.syspath_prepend(str(REAL_FILE.parent.parent.parent / "scripts"))
    import build_eia860_supplement as build

    folder = tmp_path / "2021"
    folder.mkdir()
    with zipfile.ZipFile(folder / "eia8602021.zip", "w") as bundle:
        bundle.writestr("3_1_Generator_Y2021.xlsx", b"from the archive")
    (folder / "3_1_Generator_Y2021.xlsx").write_bytes(b"left over from another download")

    _, workbook = build.fetch_workbook(2021, tmp_path)

    assert workbook.read_bytes() == b"from the archive"

"""eia_generator_key() in Python is the database's public.eia_generator_key() (ETL migration 023).

The register decides "the same generator" in Python (which older retired rows
to add) and the loader then keys the table in SQL. If the two disagreed, an
added row could collide with a base row and fail the swap, or a retired coal
unit could be added under a key its generation never joins.
"""

import re
import subprocess
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from src import eia_generator_info as register
from src.eia_generator_info import eia_generator_key

ROOT = Path(__file__).resolve().parent.parent
FIXTURE = Path(__file__).parent / "fixtures" / "eia_generator_key_023.sql"
ETL_REPO = ROOT.parent.parent / "etl" / "power-generation-etl"
EDITIONS_DIR = ROOT / "data" / "eia860"
MIGRATION = "schema/migrations/023_eia_generator_key.sql"

# Spellings the rule exists for, whatever the workbooks hold.
WRITTEN_CASES = [
    ("56298", "01"),  # Dry Fork: 923 says 1, 860 says 01
    ("6166", "MB1"),  # Rockport: MB1/MB2 are units 1/2
    ("6166", "mb2"),
    ("6166", "MB3"),  # only MB1/MB2 are rewritten
    ("1", "MB1"),  # and only at Rockport
    ("56032", "0001"),  # this plant keeps its zeros: 0001 and 001 are two turbines
    ("56032", "001"),
    ("56032", "a 1"),
    ("1", "000"),
    ("1", "0"),
    ("1", "01A"),
    ("1", "st 2"),
    ("1", " 1 "),
    ("1", "6A/B"),
    ("1", "GEN-A"),
    ("1", "1\t0"),  # only the ASCII space is removed
    ("1", "0é"),
]


def _function_sql() -> str:
    return "".join(line for line in FIXTURE.read_text().splitlines(keepends=True) if not line.startswith("--"))


def _sql_keys(pg_url, pairs: list[tuple[str, str]]) -> list[str]:
    engine = create_engine(pg_url)
    try:
        with engine.begin() as conn:
            conn.execute(text(_function_sql()))
            return list(
                conn.execute(
                    text(
                        "SELECT public.eia_generator_key(p, g) FROM unnest(CAST(:p AS text[]), CAST(:g AS text[])) "
                        "WITH ORDINALITY AS x(p, g, n) ORDER BY n"
                    ),
                    {"p": [p for p, _ in pairs], "g": [g for _, g in pairs]},
                ).scalars()
            )
    finally:
        engine.dispose()


def _assert_agree(pg_url, pairs: list[tuple[str, str]]) -> None:
    sql = _sql_keys(pg_url, pairs)
    differ = [(p, g, eia_generator_key(p, g), s) for (p, g), s in zip(pairs, sql) if eia_generator_key(p, g) != s]
    assert not differ, f"{len(differ)} of {len(pairs)} IDs keyed differently (plant, id, python, sql): {differ[:10]}"


def test_python_key_matches_the_sql_function_on_the_written_cases(pg_url):
    _assert_agree(pg_url, WRITTEN_CASES)


def test_python_key_matches_the_sql_function_on_the_committed_supplement(pg_url):
    rows = register.read_supplement(register.SUPPLEMENT_FILE)
    _assert_agree(pg_url, sorted(set(zip(rows["plant_code"], rows["generator_id"]))))


def _edition_workbooks() -> dict[int, Path]:
    books = {y: EDITIONS_DIR / str(y) / f"3_1_Generator_Y{y}.xlsx" for y in register.SUPPLEMENT_EDITIONS}
    books[register.BASE_EDITION] = register.BASE_WORKBOOK
    return books


@pytest.mark.skipif(
    not all(p.exists() for p in _edition_workbooks().values()),
    reason="Form 860 workbooks 2019-2024 not downloaded (scripts/build_eia860_supplement.py)",
)
def test_python_key_matches_the_sql_function_on_every_id_of_the_six_editions(pg_url):
    pairs = set()
    for path in _edition_workbooks().values():
        for sheet in pd.ExcelFile(path).sheet_names:  # Operable, Proposed, Retired and Canceled
            ids = pd.read_excel(path, sheet_name=sheet, skiprows=1, usecols=["Plant Code", "Generator ID"], dtype=str)
            ids = ids.dropna()
            pairs |= set(zip(ids["Plant Code"].astype(float).astype(int).astype(str), ids["Generator ID"]))
    assert len(pairs) > 35_000
    _assert_agree(pg_url, sorted(pairs))


@pytest.mark.skipif(not (ETL_REPO / ".git").exists(), reason="power-generation-etl checkout not beside plant-data")
def test_the_fixture_is_the_function_in_the_etl_repository():
    """The copy the parity tests run is the deployed definition, not a retyped one."""
    shown = subprocess.run(
        ["git", "-C", str(ETL_REPO), "show", f"origin/main:{MIGRATION}"], capture_output=True, text=True, check=False
    )
    assert shown.returncode == 0, shown.stderr
    statement = re.search(
        r"CREATE OR REPLACE FUNCTION public\.eia_generator_key\(.*?\n\$\$;\n", shown.stdout, re.DOTALL
    )
    assert statement, "migration 023 on origin/main no longer defines public.eia_generator_key"
    assert _function_sql() == statement.group(0)

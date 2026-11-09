"""Chile's all-fuel plants (ETL migrations 019, 026) in the crosswalk, the unit map
and gem_unit_monthly.

The all-fuel table labels each unit by its own fuel, so it has coal units the
coal table never had: TER MEJILLONES CTM1/CTM2, TER TOCOPILLA U12-U15,
TER TARAPACA, TER BOCAMINA (I). Where the ETL migrations are not applied
(production until then) nothing changes.
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from src import build_crosswalk, reviewed_unit_map  # noqa: E402
from src.reviewed_unit_map import SOURCES, build_rows, load_reviewed  # noqa: E402

CHILE = next(src for src in SOURCES if src.system == "CHILE")


class _Conn:
    """A connection that answers to_regclass from a set of existing tables and
    records every SQL statement."""

    def __init__(self, tables):
        self.tables, self.sql = set(tables), []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, statement, params=None):
        sql = str(statement)
        self.sql.append(sql)
        name = (params or {}).get("t") or (
            "chile_generation_all_fuels" if "chile_generation_all_fuels" in sql else None
        )

        class _Result:
            def scalar(_self):
                return name in self.tables

        return _Result()


class _Engine:
    def __init__(self, tables):
        self.conn = _Conn(tables)

    def connect(self):
        return self.conn


def _read_sql_recorder(monkeypatch, module, columns):
    seen = []

    def read_sql(sql, conn, params=None):
        seen.append(str(sql))
        return pd.DataFrame(columns=columns)

    monkeypatch.setattr(module.pd, "read_sql", read_sql)
    return seen


def test_unit_map_reads_the_all_fuel_table_only_where_it_exists(monkeypatch):
    seen = _read_sql_recorder(monkeypatch, reviewed_unit_map, ["plant", "source_id"])
    CHILE.pull_names(_Engine({"chile_generation_data"}))
    CHILE.pull_names(_Engine({"chile_generation_data", "chile_generation_all_fuels"}))
    assert "chile_generation_all_fuels" not in seen[0]
    assert "FROM chile_generation_data" in seen[1] and "FROM chile_generation_all_fuels" in seen[1]
    assert seen[1].count("fuel_type = :coal") == 2  # coal units only, from both tables


def test_crosswalk_names_come_from_the_all_fuel_table_only_where_it_exists(monkeypatch):
    seen = _read_sql_recorder(monkeypatch, build_crosswalk, ["plant_name"])
    build_crosswalk.pull_plant_names(_Engine(set()), sources=["CHILE"])
    build_crosswalk.pull_plant_names(_Engine({"chile_generation_all_fuels"}), sources=["CHILE"])
    assert "chile_generation_all_fuels" not in seen[0]
    assert "chile_generation_all_fuels" in seen[1] and "fuel_type = 'Carbón'" in seen[1]


def test_the_new_coal_plants_are_reviewed_to_their_gem_units():
    reviewed = load_reviewed(CHILE).set_index("source_id")
    assert reviewed.loc["386", "gem_unit_ids"] == "G100000106831 G100000106832"  # Mejillones 1-2
    assert reviewed.loc["387", "gem_location_id"] == "L100000100186"  # Tocopilla, not Nueva Tocopilla
    assert reviewed.loc["378", "gem_location_id"] == "L100000100180"  # Tarapacá = GEM Patache
    assert reviewed.loc["117", "gem_unit_ids"] == "G100000100976"  # Bocamina Unit 1
    assert "182" not in reviewed.index  # Petropower: petcoke, not a coal plant


def test_a_new_coal_plant_is_placed_on_its_units_and_existing_ones_keep_theirs():
    names = pd.DataFrame(
        [("TER MEJILLONES", "386"), ("TER IEM", "488"), ("TER ANDINA", "376")],
        columns=["plant", "source_id"],
    )
    out = build_rows(CHILE, names, load_reviewed(CHILE), {}).set_index("plant_key")
    assert out.loc["TER MEJILLONES", "gem_unit_ids"] == "G100000106831 G100000106832"
    assert out.loc["TER IEM", "gem_unit_id"] == "G100000106833"
    assert out.loc["TER ANDINA", "gem_unit_id"] == "G100000100194"


def test_the_fuel_group_branch_replaces_the_chile_coal_branch():
    from bootstrap_neon_db import GEM_UNIT_BRANCHES, gem_unit_branches

    both = {"mv_chile_plant_monthly", "mv_chile_fuel_group_monthly", "mv_ons_plant_monthly"}
    assert gem_unit_branches(both) == [
        n for n in GEM_UNIT_BRANCHES if n in both - {"mv_chile_plant_monthly"}
    ]
    assert "mv_chile_plant_monthly" in gem_unit_branches({"mv_chile_plant_monthly"})
    # Until the all-fuel history covers every coal month, the coal table stays.
    partial = gem_unit_branches(both, {"mv_chile_fuel_group_monthly": ["2021-03"]})
    assert "mv_chile_plant_monthly" in partial and "mv_chile_fuel_group_monthly" not in partial
    # Other fuels sit at the plant only: no GEM unit outside the coal group.
    branch = GEM_UNIT_BRANCHES["mv_chile_fuel_group_monthly"]
    assert "CASE WHEN u.fuel_group = 'coal' THEN m.gem_unit_id END" in branch
    assert "u.fuel_group = 'coal' AS is_coal" in branch
    assert "ELSE 'plant-other-fuel'" in branch


def test_coverage_counts_every_plant_month_and_its_energy():
    # Rig review of PR #40: a month counted as covered when any plant had coal.
    from bootstrap_neon_db import missing_coverage

    old = [("TER ANDINA", "2024-01", 100.0), ("TER HORNITOS", "2024-01", 200.0)]
    assert missing_coverage(old, [("TER ANDINA", "2024-01", 100.0)]) == ["TER HORNITOS 2024-01"]
    assert missing_coverage(old, [("TER ANDINA", "2024-01", 1.0), ("TER HORNITOS", "2024-01", 200.0)]) == [
        "TER ANDINA 2024-01"
    ]  # a partly loaded month
    full = [("TER ANDINA", "2024-01", 102.8), ("TER HORNITOS", "2024-01", 199.0)]
    assert missing_coverage(old, full) == []  # the repeated April hour, rounding


class _CoverageConn:
    """Answers to_regclass with True and each coverage query with given rows."""

    def __init__(self, old, new):
        from bootstrap_neon_db import COVERAGE_SQL

        self.rows = {COVERAGE_SQL["mv_chile_plant_monthly"]: old, COVERAGE_SQL["mv_chile_fuel_group_monthly"]: new}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, statement, params=None):
        rows = self.rows.get(str(statement))

        class _Result:
            def scalar(_self):
                return True

            def all(_self):
                return rows

        return _Result()


def test_the_view_sql_keeps_the_coal_table_until_every_plant_month_is_covered():
    import bootstrap_neon_db as boot

    old = [("TER ANDINA", "2024-01", 100.0), ("TER HORNITOS", "2024-01", 200.0)]

    class _Engine:
        def __init__(self, new):
            self.new = new

        def connect(self):
            return _CoverageConn(old, self.new)

    partial = boot.gem_unit_rows_sql(_Engine([("TER ANDINA", "2024-01", 100.0)]))
    assert "FROM mv_chile_plant_monthly" in partial and "FROM mv_chile_fuel_group_monthly" not in partial
    complete = boot.gem_unit_rows_sql(_Engine(old))
    assert "FROM mv_chile_fuel_group_monthly" in complete and "FROM mv_chile_plant_monthly" not in complete


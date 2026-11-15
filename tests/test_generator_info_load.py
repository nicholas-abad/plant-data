"""Loading eia_generator_info: inputs are required, and the review view survives the swap."""

from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from src import eia_generator_info as register
from tests.test_eia_generator_info import COAL, GAS, _workbook

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
KEY_FUNCTION = Path(__file__).parent / "fixtures" / "eia_generator_key_023.sql"
# Like staging: the register belongs to one role, the crosswalk and its review view to another
# that the first is a member of; a third role reads the view.
REGISTER_OWNER, CROSSWALK_OWNER, REVIEWER = "pd_test_register_owner", "pd_test_crosswalk_owner", "pd_test_reviewer"


@pytest.fixture
def bootstrap(monkeypatch):
    monkeypatch.syspath_prepend(str(SCRIPTS))
    import bootstrap_neon_db

    return bootstrap_neon_db


@pytest.fixture
def inputs(tmp_path, monkeypatch):
    """A base workbook (one operating coal unit) and a supplement (Zimmer, retired 2022)."""
    base = _workbook(tmp_path, [[3122, "1", COAL, "ST", "BIT", 660.0, "OP"]], [])
    supplement = tmp_path / "supplement.csv"
    pd.DataFrame(
        [["6019", "ST1", COAL, "ST", "RC", 1425.6, "RE", 2022]], columns=register.TABLE_COLUMNS
    ).to_csv(supplement, index=False)
    monkeypatch.setattr(register, "BASE_WORKBOOK", base)
    monkeypatch.setattr(register, "SUPPLEMENT_FILE", supplement)
    return base, supplement


class NoDatabase:
    """Any use of the engine is a failure: these paths must stop before touching the database."""

    def __getattr__(self, name):
        raise AssertionError(f"the database was used ({name})")


@pytest.mark.parametrize("gone", ["BASE_WORKBOOK", "SUPPLEMENT_FILE"])
def test_a_missing_input_fails_the_register_load(bootstrap, inputs, monkeypatch, tmp_path, gone):
    """It used to print SKIP and exit 0, which reads as a finished load."""
    monkeypatch.setattr(register, gone, tmp_path / "not-downloaded.xlsx")
    with pytest.raises(FileNotFoundError, match="NOT reloaded: not-downloaded.xlsx"):
        bootstrap.load_eia_generator_info(NoDatabase())


def test_the_combined_load_keeps_going_without_the_workbook(bootstrap, inputs, monkeypatch, tmp_path, capsys):
    """--data-only reloads the crosswalk on machines that never downloaded Form 860."""
    monkeypatch.setattr(register, "BASE_WORKBOOK", tmp_path / "not-downloaded.xlsx")
    calls = []
    for name in ("load_unified_crosswalk", "load_gcpt_coal_metadata", "load_unit_gem_map"):
        monkeypatch.setattr(bootstrap, name, lambda engine, name=name: calls.append(name))
    bootstrap.load_all_reference_data(NoDatabase())
    assert calls == ["load_unified_crosswalk", "load_gcpt_coal_metadata", "load_unit_gem_map"]
    assert "WARN  eia_generator_info NOT reloaded" in capsys.readouterr().out


# --- with a local PostgreSQL ---------------------------------------------------


def _database(pg_url, with_view: bool):
    """A staging-shaped database; returns the register owner's engine and an admin engine."""
    admin = create_engine(pg_url)
    stubs = {
        "mv_entsoe_plant_monthly": "plant_name text, fuel_type text",
        "mv_npp_plant_monthly": "plant text, fuel_type text",
        "mv_ons_plant_monthly": "plant text, fuel_type text",
        "mv_occto_plant_monthly": "plant text, fuel_type text",
        "mv_chile_plant_monthly": "plant text, fuel_type text",
        "oe_facility_generation_data": "facility_name text, fueltech text",
        "mv_climatetrace_coal_monthly": "climatetrace_id text",
    }
    candidates = ", ".join(f"candidate_{i}_id text, candidate_{i}_name text, candidate_{i}_score float" for i in (1, 2, 3))
    with admin.begin() as conn:
        for role, login in ((REGISTER_OWNER, "LOGIN PASSWORD 'test'"), (CROSSWALK_OWNER, "NOLOGIN"), (REVIEWER, "LOGIN PASSWORD 'test'")):
            conn.execute(text(
                f"DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}') "
                f"THEN CREATE ROLE {role} {login}; END IF; END $$"
            ))
        conn.execute(text(f"GRANT {CROSSWALK_OWNER} TO {REGISTER_OWNER}"))
        conn.execute(text(f"GRANT ALL ON SCHEMA public TO {REGISTER_OWNER}, {CROSSWALK_OWNER}"))
        conn.execute(text(f"GRANT USAGE ON SCHEMA public TO {REVIEWER}"))
        conn.execute(text(KEY_FUNCTION.read_text()))
        for name, columns in stubs.items():
            conn.execute(text(f"CREATE TABLE {name} ({columns})"))
            conn.execute(text(f"GRANT SELECT ON {name} TO {CROSSWALK_OWNER}"))
        conn.execute(text(
            "CREATE TABLE plant_crosswalk (source_system text, plant_code text, plant_name text, source_country text, "
            f"latitude float, decided_by text, note text, {candidates}, gem_location_id text, not_in_gem boolean)"
        ))
        conn.execute(text(
            "INSERT INTO plant_crosswalk (source_system, plant_code, plant_name, not_in_gem) VALUES "
            "('EIA', '6019', 'W H Zimmer', false), ('EIA', '999', 'Gas Plant', false)"
        ))
        conn.execute(text(f"ALTER TABLE plant_crosswalk OWNER TO {CROSSWALK_OWNER}"))
        # The register as the old code left it: the 2024 edition only, Zimmer missing.
        conn.execute(text(
            "CREATE TABLE eia_generator_info (plant_code text, generator_id text, technology text)"
        ))
        conn.execute(text(f"INSERT INTO eia_generator_info VALUES ('3122', '1', '{COAL}'), ('999', '1', '{GAS}')"))
        conn.execute(text(f"ALTER TABLE eia_generator_info OWNER TO {REGISTER_OWNER}"))
        conn.execute(text(f"GRANT SELECT ON eia_generator_info TO {CROSSWALK_OWNER}"))
        if with_view:
            conn.execute(text(f"SET ROLE {CROSSWALK_OWNER}"))
            import bootstrap_neon_db

            for sql in bootstrap_neon_db.REVIEW_VIEW_SQL:
                conn.execute(text(sql))
            conn.execute(text(f"GRANT SELECT ON plant_crosswalk_review TO {REVIEWER}"))
            conn.execute(text("RESET ROLE"))
    return create_engine(pg_url.set(username=REGISTER_OWNER, password="test")), admin


def _view(admin):
    """(owner, roles with SELECT besides the owner) of plant_crosswalk_review, or None."""
    with admin.connect() as conn:
        owner = conn.execute(text(
            "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE relname = 'plant_crosswalk_review' AND relkind = 'v'"
        )).scalar()
        if owner is None:
            return None
        readers = conn.execute(text(
            "SELECT grantee FROM information_schema.role_table_grants WHERE table_name = 'plant_crosswalk_review' "
            "AND privilege_type = 'SELECT' AND grantee <> :owner ORDER BY 1"
        ), {"owner": owner}).scalars().all()
        return owner, readers


def test_the_review_view_survives_a_register_load_with_its_owner_and_grants(bootstrap, inputs, pg_url):
    """The swap's DROP ... CASCADE used to leave no view until the next crosswalk load."""
    engine, admin = _database(pg_url, with_view=True)
    try:
        assert _view(admin) == (CROSSWALK_OWNER, [REVIEWER])

        bootstrap.load_eia_generator_info(engine)

        assert _view(admin) == (CROSSWALK_OWNER, [REVIEWER])
        reviewer = create_engine(pg_url.set(username=REVIEWER, password="test"))
        with reviewer.connect() as conn:
            rows = conn.execute(text(
                "SELECT plant_code, coal_relevant FROM plant_crosswalk_review ORDER BY plant_code"
            )).fetchall()
        reviewer.dispose()
        # It reads the NEW register: Zimmer is coal again; the gas plant's old row is gone.
        assert [tuple(r) for r in rows] == [("6019", True), ("999", False)]
        with admin.connect() as conn:
            loaded = conn.execute(text(
                "SELECT plant_code, generator_id, id_as_registered, status, source_edition "
                "FROM eia_generator_info ORDER BY source_edition"
            )).fetchall()
        assert [tuple(r) for r in loaded] == [("6019", "ST1", "ST1", "RE", 2022), ("3122", "1", "1", "OP", 2024)]
    finally:
        engine.dispose()
        admin.dispose()


def test_a_register_load_brings_back_a_view_an_earlier_load_dropped(bootstrap, inputs, pg_url):
    """No view to copy an owner from: it gets plant_crosswalk's owner, who creates it every week."""
    engine, admin = _database(pg_url, with_view=False)
    try:
        assert _view(admin) is None
        bootstrap.load_eia_generator_info(engine)
        assert _view(admin) == (CROSSWALK_OWNER, [])
    finally:
        engine.dispose()
        admin.dispose()


def test_a_register_load_without_a_crosswalk_creates_no_view(bootstrap, inputs, pg_url):
    """A fresh database loads the register before any plant_crosswalk exists."""
    engine, admin = _database(pg_url, with_view=False)
    try:
        with admin.begin() as conn:
            conn.execute(text("DROP TABLE plant_crosswalk"))
        bootstrap.load_eia_generator_info(engine)
        assert _view(admin) is None
    finally:
        engine.dispose()
        admin.dispose()

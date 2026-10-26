"""import_decisions.py --replace-file against a throwaway SQLite crosswalk (no Neon)."""

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.pool import StaticPool

CANDIDATES = ", ".join(f"candidate_{i}_{c} TEXT" for i in (1, 2, 3) for c in ("id", "name", "score"))


@pytest.fixture
def mod():
    spec = importlib.util.spec_from_file_location(
        "import_decisions",
        Path(__file__).resolve().parents[1] / "scripts" / "import_decisions.py",
    )
    m = importlib.util.module_from_spec(spec)
    sys.modules["import_decisions"] = m
    spec.loader.exec_module(m)
    return m


@pytest.fixture
def db(mod, monkeypatch):
    """Three ENTSO-E rows: a rapidfuzz link, a legacy-frozen open row, and a
    name with a trailing space, as production's feed spells Wilhelmshaven."""
    engine = create_engine("sqlite://", poolclass=StaticPool)

    # pysqlite's own transaction handling breaks SAVEPOINT/ROLLBACK (the
    # importer's nested savepoints); SQLAlchemy's documented fix: let
    # SQLAlchemy emit BEGIN itself.
    @event.listens_for(engine, "connect")
    def _no_pysqlite_tx(dbapi_conn, _):
        dbapi_conn.isolation_level = None

    @event.listens_for(engine, "begin")
    def _begin(conn):
        conn.exec_driver_sql("BEGIN")

    with engine.begin() as c:
        c.execute(text("CREATE TABLE gem_locations (gem_location_id TEXT, name TEXT, country TEXT)"))
        c.execute(
            text(
                "CREATE TABLE plant_crosswalk (source_system TEXT, plant_code TEXT, plant_name TEXT, "
                "gem_location_id TEXT, not_in_gem BOOLEAN, matching_method TEXT, decided_by TEXT, "
                "decided_on TEXT, note TEXT, override_reason TEXT, gem_name_at_decision TEXT, "
                f"gem_country_at_decision TEXT, {CANDIDATES})"
            )
        )
        c.execute(
            text(
                "INSERT INTO gem_locations VALUES ('LGAS', 'Gas station', 'Montenegro'), "
                "('LCOAL', 'Coal station', 'Montenegro'), ('LWHV', 'Wilhelmshaven', 'Germany')"
            )
        )
        c.execute(
            text(
                "INSERT INTO plant_crosswalk (source_system, plant_name, gem_location_id, not_in_gem, "
                "matching_method, decided_by, candidate_1_id) VALUES "
                "('ENTSOE', 'Pljevlja', 'LGAS', 0, 'rapidfuzz', NULL, 'LX'), "
                "('ENTSOE', 'Zolling', NULL, 0, 'legacy', 'legacy-pipeline', NULL), "
                "('ENTSOE', 'WHV ', NULL, 0, NULL, NULL, NULL)"
            )
        )
    monkeypatch.setattr(mod, "get_engine", lambda: engine)
    return engine


def manifest(tmp_path, rows):
    path = tmp_path / "m.csv"
    pd.DataFrame(rows, columns=["source_system", "key", "old_gem_location_id", "old_decided_by",
                                "gem_location_id", "decided_by", "note"]).to_csv(path, index=False)
    return path


def run(mod, path, commit):
    return mod.replace_from_manifest(SimpleNamespace(replace_file=path, by="T. Est", commit=commit))


def rows(engine):
    with engine.connect() as c:
        return pd.read_sql("SELECT * FROM plant_crosswalk", c).set_index("plant_name")


GOOD = [
    ("ENTSOE", "Pljevlja", "LGAS", "", "LCOAL", "review-2026", "coal plant"),
    ("ENTSOE", "Zolling", "", "legacy-pipeline", "LCOAL", "review-2026", "frozen open row"),
    ("ENTSOE", "WHV ", "", "", "LWHV", "review-2026", "trailing space"),
]


def test_dry_run_writes_nothing(mod, db, tmp_path):
    before = rows(db)
    assert run(mod, manifest(tmp_path, GOOD), commit=False) == 0
    pd.testing.assert_frame_equal(rows(db), before)


def test_commit_applies_every_row_verbatim(mod, db, tmp_path):
    assert run(mod, manifest(tmp_path, GOOD), commit=True) == 0
    out = rows(db)
    assert out.loc["Pljevlja", "gem_location_id"] == "LCOAL"
    assert out.loc["Pljevlja", "decided_by"] == "review-2026"
    assert out.loc["Pljevlja", "matching_method"] == "manual"
    assert pd.isna(out.loc["Pljevlja", "candidate_1_id"])
    assert out.loc["Pljevlja", "gem_name_at_decision"] == "Coal station"
    assert out.loc["Pljevlja", "note"] == "coal plant (applied by T. Est)"
    assert out.loc["Zolling", "gem_location_id"] == "LCOAL"
    # the key keeps its trailing space end to end: read, matched, written
    assert out.loc["WHV ", "gem_location_id"] == "LWHV"


def test_one_stale_row_blocks_the_whole_manifest(mod, db, tmp_path):
    stale = GOOD + [("ENTSOE", "Pljevlja ", "LGAS", "", "LCOAL", "review-2026", "no such key")]
    before = rows(db)
    assert run(mod, manifest(tmp_path, stale), commit=True) == 1
    pd.testing.assert_frame_equal(rows(db), before)


def test_old_values_must_match_exactly(mod, db, tmp_path):
    # Zolling is legacy-pipeline, so an expected old decided_by of '' is stale
    wrong = [("ENTSOE", "Zolling", "", "", "LCOAL", "review-2026", "x")]
    assert run(mod, manifest(tmp_path, wrong), commit=True) == 1
    assert pd.isna(rows(db).loc["Zolling", "gem_location_id"])


def test_unknown_gem_location_is_refused(mod, db, tmp_path):
    unknown = [("ENTSOE", "Pljevlja", "LGAS", "", "LNOPE", "review-2026", "x")]
    assert run(mod, manifest(tmp_path, unknown), commit=True) == 1
    assert rows(db).loc["Pljevlja", "gem_location_id"] == "LGAS"


def test_malformed_manifests_stop_before_the_database(mod, tmp_path, monkeypatch):
    monkeypatch.setattr(mod, "get_engine", lambda: pytest.fail("must not connect"))
    path = tmp_path / "bad.csv"
    pd.DataFrame({"source_system": ["ENTSOE"], "key": ["X"]}).to_csv(path, index=False)
    assert run(mod, path, commit=True) == 2
    assert run(mod, manifest(tmp_path, [GOOD[0], GOOD[0]]), commit=True) == 2


@pytest.mark.parametrize("name", sorted(p.name for p in (
    Path(__file__).resolve().parents[1] / "data" / "crosswalks" / "decision_migrations").glob("*.csv")))
def test_committed_manifests_are_well_formed(mod, name):
    path = Path(__file__).resolve().parents[1] / "data" / "crosswalks" / "decision_migrations" / name
    m = pd.read_csv(path, dtype=str, keep_default_na=False)
    assert list(m.columns) == mod.MANIFEST_COLUMNS
    assert not m.duplicated(["source_system", "key"]).any()
    assert m["gem_location_id"].str.fullmatch(r"L\d+").all()
    assert (m["old_gem_location_id"] != m["gem_location_id"]).all()

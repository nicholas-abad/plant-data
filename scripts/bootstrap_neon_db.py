#!/usr/bin/env python3
"""
Bootstrap a Neon (PostgreSQL) database with generation schema and unified crosswalk.

Usage:
    uv run --extra db python scripts/bootstrap_neon_db.py            # Full bootstrap
    uv run --extra db python scripts/bootstrap_neon_db.py --schema-only  # Schema only
    uv run --extra db python scripts/bootstrap_neon_db.py --data-only    # Crosswalk data only
    uv run --extra db python scripts/bootstrap_neon_db.py --test-only    # NPP LLM test data only

IMPORTANT: Run build_crosswalk.py BEFORE this script to produce
unified_plant_crosswalk.parquet (while GPPD/reference tables still exist in Neon).
"""

import argparse
import os
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR.parent / "data"
SCHEMA_DIR = SCRIPT_DIR.parent.parent.parent / "etl" / "power-generation-etl" / "schema"

SCHEMA_FILES = [
    "extraction_metadata.sql",
    "eia_generation.sql",
    "entsoe_generation.sql",
    "npp_generation.sql",
    "ons_generation.sql",
    "oe_generation.sql",
    "oe_facility_generation.sql",
    "occto_generation.sql",
    "materialized_views.sql",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def get_engine():
    """Create a SQLAlchemy engine from environment variables."""
    load_dotenv(SCRIPT_DIR.parent / ".env")

    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "power_generation")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "")
    sslmode = os.getenv("POSTGRES_SSLMODE", "")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    if sslmode:
        url += f"?sslmode={sslmode}"

    return create_engine(url)


# ---------------------------------------------------------------------------
# Schema creation
# ---------------------------------------------------------------------------


def create_schema(engine) -> bool:
    """Execute generation-table and materialized-view SQL from the ETL repo."""
    if not SCHEMA_DIR.exists():
        print(f"  WARNING: Schema directory not found at {SCHEMA_DIR}")
        print("  Skipping schema creation. Ensure the ETL repo is checked out alongside plant-data.")
        return False

    for filename in SCHEMA_FILES:
        path = SCHEMA_DIR / filename
        if not path.exists():
            print(f"  WARNING: {filename} not found, skipping")
            continue

        sql = path.read_text()
        with engine.connect() as conn:
            conn.execute(text(sql))
            conn.commit()
        print(f"  OK  {filename}")

    return True


# ---------------------------------------------------------------------------
# Reference-data loading
# ---------------------------------------------------------------------------


def load_unified_crosswalk(engine):
    """Load the unified plant crosswalk parquet into Neon as plant_crosswalk."""
    path = DATA_DIR / "crosswalks" / "unified_plant_crosswalk.parquet"
    if not path.exists():
        print(f"  SKIP  {path.name} not found — run build_crosswalk.py first")
        return

    df = pd.read_parquet(path)

    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS plant_crosswalk CASCADE"))
        conn.commit()

    df.to_sql("plant_crosswalk", engine, index=False)

    with engine.connect() as conn:
        # EIA has duplicate plant_names (different plant_codes), so use
        # COALESCE(plant_code, plant_name) to ensure uniqueness per source
        conn.execute(text(
            "CREATE UNIQUE INDEX idx_plant_crosswalk_pk "
            "ON plant_crosswalk (COALESCE(plant_code, plant_name), source_system)"
        ))
        conn.execute(text(
            "CREATE INDEX idx_plant_crosswalk_source "
            "ON plant_crosswalk (source_system)"
        ))
        conn.execute(text(
            "CREATE INDEX idx_plant_crosswalk_plant_code "
            "ON plant_crosswalk (plant_code) WHERE plant_code IS NOT NULL"
        ))
        conn.execute(text(
            "CREATE INDEX idx_plant_crosswalk_plant_name "
            "ON plant_crosswalk (plant_name, source_system)"
        ))
        conn.commit()

    print(f"  OK  plant_crosswalk: {len(df):,} rows")


def load_npp_llm_test(engine):
    """Load the NPP LLM test parquet into Neon as npp_llm_test."""
    path = DATA_DIR / "crosswalks" / "npp_llm_test.parquet"
    if not path.exists():
        print(f"  SKIP  {path.name} not found — run test_npp_llm.py first")
        return

    df = pd.read_parquet(path)

    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS npp_llm_test CASCADE"))
        conn.commit()

    df.to_sql("npp_llm_test", engine, index=False)

    with engine.connect() as conn:
        conn.execute(text(
            "ALTER TABLE npp_llm_test "
            "ADD PRIMARY KEY (plant_name, source_system)"
        ))
        conn.commit()

    print(f"  OK  npp_llm_test: {len(df):,} rows")


def load_all_reference_data(engine):
    """Load the unified crosswalk table."""
    load_unified_crosswalk(engine)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Bootstrap a Neon PostgreSQL database with generation schema and crosswalk data.",
    )
    parser.add_argument(
        "--schema-only",
        action="store_true",
        help="Only create generation tables and materialized views (skip data load)",
    )
    parser.add_argument(
        "--data-only",
        action="store_true",
        help="Only load unified crosswalk data (skip schema creation)",
    )
    parser.add_argument(
        "--test-only",
        action="store_true",
        help="Only load NPP LLM test data (npp_llm_test table)",
    )
    args = parser.parse_args()

    mutually_exclusive = sum([args.schema_only, args.data_only, args.test_only])
    if mutually_exclusive > 1:
        print("ERROR: --schema-only, --data-only, and --test-only are mutually exclusive")
        sys.exit(1)

    engine = get_engine()

    # Quick connectivity check
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("Connected to database.\n")
    except Exception as e:
        print(f"ERROR: Could not connect to database: {e}")
        sys.exit(1)

    if args.test_only:
        print("Loading NPP LLM test data...")
        load_npp_llm_test(engine)
        print()
    else:
        if not args.data_only:
            print("Creating schema (generation tables + materialized views)...")
            create_schema(engine)
            print()

        if not args.schema_only:
            print("Loading reference data...")
            load_all_reference_data(engine)
            print()

    print("Done.")
    engine.dispose()


if __name__ == "__main__":
    main()

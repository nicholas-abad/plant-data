#!/usr/bin/env python3
"""
Bootstrap a Neon (PostgreSQL) database with generation schema and reference/crosswalk data.

Usage:
    uv run --extra db python scripts/load_to_neon.py            # Full bootstrap
    uv run --extra db python scripts/load_to_neon.py --schema-only  # Schema only
    uv run --extra db python scripts/load_to_neon.py --data-only    # Reference data only
    uv run --extra db python scripts/load_to_neon.py --skip-gcpt    # Skip large GCPT table

Requires a .env file (copy from .env.template) with Neon credentials.
"""

import argparse
import os
import re
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


def sanitize_column_name(name: str) -> str:
    """Convert a messy CSV header to a clean snake_case column name.

    Examples:
        "Country/Area"          -> "country_area"
        "EIA plant + unit ID"   -> "eia_plant_unit_id"
        "H2 ready turbine (%)?" -> "h2_ready_turbine_pct"
        "Owner(s) GEM Entity ID" -> "owner_s_gem_entity_id"
    """
    s = name.strip()
    # Replace (%) with pct
    s = s.replace("(%)", "pct")
    # Replace non-alphanumeric chars with underscores
    s = re.sub(r"[^a-zA-Z0-9]+", "_", s)
    # Collapse runs of underscores and strip edges
    s = re.sub(r"_+", "_", s).strip("_")
    return s.lower()


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


def _load_table(engine, table_name: str, df: pd.DataFrame, pk_sql: str | None = None):
    """Drop-and-replace a reference table, optionally adding a primary key."""
    with engine.connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
        conn.commit()

    df.to_sql(table_name, engine, index=False)

    if pk_sql:
        with engine.connect() as conn:
            conn.execute(text(pk_sql))
            conn.commit()

    print(f"  OK  {table_name}: {len(df):,} rows")


def load_eia_coordinates(engine):
    path = DATA_DIR / "crosswalks" / "eia_plant_coordinates.parquet"
    if not path.exists():
        print(f"  SKIP  {path.name} not found")
        return
    df = pd.read_parquet(path)
    # Filter to rows with valid eia_plant_unit_id (format: "digits|alphanumeric")
    # Drops Excel #VALUE! errors and non-standard entries like "52071, EIA: 6648|GEN1"
    valid_mask = df["eia_plant_unit_id"].str.match(r"^\d+\|.+$", na=False)
    dropped = len(df) - valid_mask.sum()
    if dropped:
        print(f"  INFO  Dropping {dropped} rows with invalid eia_plant_unit_id")
    df = df[valid_mask].drop_duplicates(subset="eia_plant_unit_id")
    _load_table(
        engine,
        "plant_coordinates_eia",
        df,
        pk_sql="ALTER TABLE plant_coordinates_eia ADD PRIMARY KEY (eia_plant_unit_id)",
    )


def load_entsoe_coordinates(engine):
    path = DATA_DIR / "crosswalks" / "entsoe_plant_coordinates.parquet"
    if not path.exists():
        print(f"  SKIP  {path.name} not found")
        return
    df = pd.read_parquet(path)
    _load_table(
        engine,
        "plant_coordinates_entsoe",
        df,
        pk_sql="ALTER TABLE plant_coordinates_entsoe ADD PRIMARY KEY (project_name, unit_name)",
    )


def load_npp_coordinates(engine):
    path = DATA_DIR / "crosswalks" / "npp_plant_coordinates.parquet"
    if not path.exists():
        print(f"  SKIP  {path.name} not found")
        return
    df = pd.read_parquet(path)
    _load_table(
        engine,
        "plant_coordinates_npp",
        df,
        pk_sql="ALTER TABLE plant_coordinates_npp ADD PRIMARY KEY (project_name, unit_name)",
    )


def load_gcpt(engine):
    path = DATA_DIR / "gcpt" / "gcpt_global_2025.csv"
    if not path.exists():
        print(f"  SKIP  {path.name} not found")
        return
    df = pd.read_csv(path, low_memory=False)
    df.columns = [sanitize_column_name(c) for c in df.columns]
    _load_table(
        engine,
        "gcpt_global",
        df,
        pk_sql="ALTER TABLE gcpt_global ADD PRIMARY KEY (unit_id)",
    )


def load_npp_gipt_crosswalk(engine):
    path = DATA_DIR / "crosswalks" / "NPP_GIPT_crosswalk.csv"
    if not path.exists():
        print(f"  SKIP  {path.name} not found")
        return
    df = pd.read_csv(path)
    df.columns = [sanitize_column_name(c) for c in df.columns]
    # Add a serial-style integer PK
    df.insert(0, "id", range(1, len(df) + 1))
    _load_table(
        engine,
        "npp_gipt_crosswalk",
        df,
        pk_sql="ALTER TABLE npp_gipt_crosswalk ADD PRIMARY KEY (id)",
    )


def load_gppd(engine):
    """Load the WRI Global Power Plant Database into Neon."""
    # Look in the dashboard data directory
    dashboard_path = SCRIPT_DIR.parent.parent.parent / "dashboard" / "energy-generation-dashboard" / "data" / "global_power_plant_database.csv"
    if not dashboard_path.exists():
        print(f"  SKIP  global_power_plant_database.csv not found at {dashboard_path}")
        return
    df = pd.read_csv(dashboard_path, low_memory=False)
    _load_table(
        engine,
        "gppd_global",
        df,
        pk_sql="ALTER TABLE gppd_global ADD PRIMARY KEY (gppd_idnr)",
    )


def load_all_reference_data(engine, skip_gcpt: bool = False):
    """Load all crosswalk and GCPT reference tables."""
    load_eia_coordinates(engine)
    load_entsoe_coordinates(engine)
    load_npp_coordinates(engine)
    load_npp_gipt_crosswalk(engine)
    load_gppd(engine)

    if skip_gcpt:
        print("  SKIP  gcpt_global (--skip-gcpt)")
    else:
        load_gcpt(engine)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Bootstrap a Neon PostgreSQL database with generation schema and reference data.",
    )
    parser.add_argument(
        "--schema-only",
        action="store_true",
        help="Only create generation tables and materialized views (skip data load)",
    )
    parser.add_argument(
        "--data-only",
        action="store_true",
        help="Only load crosswalk/GCPT reference data (skip schema creation)",
    )
    parser.add_argument(
        "--skip-gcpt",
        action="store_true",
        help="Skip loading the GCPT global table (~14K rows)",
    )
    args = parser.parse_args()

    if args.schema_only and args.data_only:
        print("ERROR: --schema-only and --data-only are mutually exclusive")
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

    if not args.data_only:
        print("Creating schema (generation tables + materialized views)...")
        create_schema(engine)
        print()

    if not args.schema_only:
        print("Loading reference data...")
        load_all_reference_data(engine, skip_gcpt=args.skip_gcpt)
        print()

    print("Done.")
    engine.dispose()


if __name__ == "__main__":
    main()

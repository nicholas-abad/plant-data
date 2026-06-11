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
from sqlalchemy.engine import URL


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR.parent / "data"
SCHEMA_DIR = SCRIPT_DIR.parent.parent.parent / "etl" / "power-generation-etl" / "schema"

SCHEMA_FILES = [
    "extraction_metadata.sql",
    "eia_generation.sql",
    "eia_generator_info.sql",
    "gcpt_coal_metadata.sql",
    "entsoe_generation.sql",
    "npp_generation.sql",
    "ons_generation.sql",
    "oe_generation.sql",
    "oe_facility_generation.sql",
    "occto_generation.sql",
    "chile_generation.sql",
    "materialized_views.sql",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def get_engine():
    """Create a SQLAlchemy engine from environment variables."""
    load_dotenv(SCRIPT_DIR.parent / ".env")

    sslmode = os.getenv("POSTGRES_SSLMODE", "")
    connection_url = URL.create(
        drivername="postgresql+psycopg2",
        username=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "power_generation"),
        query={"sslmode": sslmode} if sslmode else {},
    )
    return create_engine(connection_url)


# ---------------------------------------------------------------------------
# Schema creation
# ---------------------------------------------------------------------------


def create_schema(engine) -> bool:
    """Execute generation-table and materialized-view SQL from the ETL repo."""
    if not SCHEMA_DIR.exists():
        print(f"  WARNING: Schema directory not found at {SCHEMA_DIR}")
        print(
            "  Skipping schema creation. Ensure the ETL repo is checked out alongside plant-data."
        )
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


def _atomic_replace_table(engine, df, table: str, post_load_sql: list[str]):
    """Replace `table` with `df` without a window where it is missing or broken.

    The old drop-then-load committed the DROP first, so any failure during
    the load or the index DDL (e.g. a uniqueness violation from a bad
    parquet) left production with NO table at all. Instead: load into a
    staging table (failure here leaves prod untouched), then swap and apply
    constraints inside ONE transaction — any failure rolls the swap back
    and the previous table survives intact.

    Note: dropping the old table still uses CASCADE, so dependent views
    (none today) would be destroyed by a SUCCESSFUL swap exactly as before.
    """
    staging = f"{table}_staging"
    with engine.connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {staging} CASCADE"))
        conn.commit()

    df.to_sql(staging, engine, index=False)

    with engine.begin() as conn:
        n = conn.execute(text(f"SELECT COUNT(*) FROM {staging}")).scalar()
        if n != len(df):
            raise RuntimeError(f"{staging}: expected {len(df):,} rows, found {n:,}")
        conn.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE"))
        conn.execute(text(f"ALTER TABLE {staging} RENAME TO {table}"))
        for sql in post_load_sql:
            conn.execute(text(sql))


def load_unified_crosswalk(engine):
    """Load the unified plant crosswalk parquet into Neon as plant_crosswalk."""
    path = DATA_DIR / "crosswalks" / "unified_plant_crosswalk.parquet"
    if not path.exists():
        print(f"  SKIP  {path.name} not found — run build_crosswalk.py first")
        return

    df = pd.read_parquet(path)

    _atomic_replace_table(
        engine,
        df,
        "plant_crosswalk",
        [
            # EIA has duplicate plant_names (different plant_codes), so use
            # COALESCE(plant_code, plant_name) to ensure uniqueness per source
            "CREATE UNIQUE INDEX idx_plant_crosswalk_pk "
            "ON plant_crosswalk (COALESCE(plant_code, plant_name), source_system)",
            "CREATE INDEX idx_plant_crosswalk_source "
            "ON plant_crosswalk (source_system)",
            "CREATE INDEX idx_plant_crosswalk_plant_code "
            "ON plant_crosswalk (plant_code) WHERE plant_code IS NOT NULL",
            "CREATE INDEX idx_plant_crosswalk_plant_name "
            "ON plant_crosswalk (plant_name, source_system)",
        ],
    )

    print(f"  OK  plant_crosswalk: {len(df):,} rows")


def load_npp_llm_test(engine):
    """Load the NPP LLM test parquet into Neon as npp_llm_test."""
    path = DATA_DIR / "crosswalks" / "npp_llm_test.parquet"
    if not path.exists():
        print(f"  SKIP  {path.name} not found — run test_npp_llm.py first")
        return

    df = pd.read_parquet(path)

    _atomic_replace_table(
        engine,
        df,
        "npp_llm_test",
        [
            "ALTER TABLE npp_llm_test ADD PRIMARY KEY (plant_name, source_system)",
        ],
    )

    print(f"  OK  npp_llm_test: {len(df):,} rows")


def load_eia_generator_info(engine):
    """Load EIA Form 860 generator-level reference data (Technology, etc.)."""
    path = DATA_DIR / "crosswalks" / "3_1_Generator_Y2024.xlsx"
    if not path.exists():
        print(f"  SKIP  {path.name} not found")
        return

    df = pd.read_excel(
        path,
        skiprows=1,
        usecols=[
            "Plant Code",
            "Generator ID",
            "Technology",
            "Prime Mover",
            "Energy Source 1",
            "Nameplate Capacity (MW)",
        ],
    )
    df = df.rename(
        columns={
            "Plant Code": "plant_code",
            "Generator ID": "generator_id",
            "Technology": "technology",
            "Prime Mover": "prime_mover",
            "Energy Source 1": "energy_source_1",
            "Nameplate Capacity (MW)": "nameplate_capacity_mw",
        }
    )
    # Drop rows with null keys (trailing empty rows in the xlsx)
    df = df.dropna(subset=["plant_code", "generator_id"])
    # Ensure join-key types match eia_generation_data (VARCHAR)
    df["plant_code"] = df["plant_code"].astype(int).astype(str)
    df["generator_id"] = df["generator_id"].astype(str)

    _atomic_replace_table(
        engine,
        df,
        "eia_generator_info",
        [
            "ALTER TABLE eia_generator_info ADD PRIMARY KEY (plant_code, generator_id)",
            "CREATE INDEX idx_eia_gen_info_technology ON eia_generator_info (technology)",
        ],
    )

    print(f"  OK  eia_generator_info: {len(df):,} rows")


def load_gcpt_coal_metadata(engine):
    """Load GCPT coal metadata (coal type, technology) for CO2 emission estimation."""
    import re

    # Try GCPT-specific file first, fall back to GEM database
    gcpt_path = (
        SCRIPT_DIR.parent.parent.parent
        / "other_repositories"
        / "krv-analytics"
        / "data"
        / "28August2025_GCPT_Database.csv"
    )
    if not gcpt_path.exists():
        gcpt_path = DATA_DIR / "crosswalks" / "GEM database_21Feb2026.csv"
    if not gcpt_path.exists():
        print("  SKIP  No GCPT/GEM database found")
        return

    df = pd.read_csv(gcpt_path, low_memory=False)

    # Filter to coal plants
    df = df[df["Fuel"].str.contains("coal", case=False, na=False)].copy()

    def _extract_eia(s):
        m = re.search(r"EIA:\s*([^,]+)", str(s))
        return m.group(1).strip() if m else None

    def _extract_coal_type(fuel_str):
        """Extract primary coal type from Fuel column (e.g., 'coal: bituminous' → 'bituminous')."""
        m = re.search(r"coal:\s*([\w-]+)", str(fuel_str))
        return m.group(1) if m else "unknown"

    def _parse_capacity(cap_str):
        """Parse capacity string like '96.0 MW' → 96.0."""
        m = re.match(r"([\d.]+)", str(cap_str))
        return float(m.group(1)) if m else None

    # Build composite EIA unit ID: plant_code|generator_id
    df["plant_code"] = df["Non WEPP location IDs"].apply(_extract_eia)
    df["generator_id"] = df["Unit Other IDs"].apply(_extract_eia)
    df["eia_unit_id"] = df.apply(
        lambda r: (
            f"{r['plant_code']}|{r['generator_id']}"
            if pd.notna(r["plant_code"])
            and pd.notna(r["generator_id"])
            and str(r["plant_code"]).strip()
            and str(r["generator_id"]).strip()
            else None
        ),
        axis=1,
    )

    # Extract fields
    out = pd.DataFrame(
        {
            "gcpt_unit_id": df["Unit ID"],
            "eia_unit_id": df["eia_unit_id"],
            "plant_name": df["Project Name"],
            "unit_name": df["Unit Name"],
            "coal_type": df["Fuel"].apply(_extract_coal_type),
            "technology": df["Technology"].fillna("unknown").str.lower().str.strip(),
            "capacity_mw": df["Capacity"].apply(_parse_capacity),
            "country": df["Country/Area"],
        }
    )
    out = out.dropna(subset=["gcpt_unit_id"])

    # Deduplicate eia_unit_id — keep the record with highest capacity
    # Only dedup rows WITH an eia_unit_id; leave NULLs untouched
    has_eia = out[out["eia_unit_id"].notna()]
    no_eia = out[out["eia_unit_id"].isna()]
    n_dupes = has_eia.duplicated(subset=["eia_unit_id"], keep=False).sum()
    if n_dupes > 0:
        print(
            f"  WARN  {n_dupes} rows with duplicate eia_unit_id — keeping highest capacity"
        )
    has_eia = has_eia.sort_values(
        "capacity_mw", ascending=False, na_position="last"
    ).drop_duplicates(subset=["eia_unit_id"], keep="first")
    out = pd.concat([has_eia, no_eia], ignore_index=True)
    if len(no_eia) > 0:
        print(f"  INFO  {len(no_eia)} rows without EIA unit ID (non-USA or unmatched)")

    _atomic_replace_table(
        engine,
        out,
        "gcpt_coal_metadata",
        [
            "ALTER TABLE gcpt_coal_metadata ADD PRIMARY KEY (gcpt_unit_id)",
            "CREATE INDEX idx_gcpt_coal_eia_unit "
            "ON gcpt_coal_metadata (eia_unit_id) WHERE eia_unit_id IS NOT NULL",
        ],
    )

    usa_with_eia = out[(out["country"] == "United States") & out["eia_unit_id"].notna()]
    print(
        f"  OK  gcpt_coal_metadata: {len(out):,} rows ({len(usa_with_eia):,} USA with EIA IDs)"
    )


def load_all_reference_data(engine):
    """Load the unified crosswalk table, EIA generator info, and GCPT coal metadata."""
    load_unified_crosswalk(engine)
    load_eia_generator_info(engine)
    load_gcpt_coal_metadata(engine)


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
    parser.add_argument(
        "--generator-info-only",
        action="store_true",
        help="Only load EIA Form 860 generator info (eia_generator_info table)",
    )
    parser.add_argument(
        "--gcpt-only",
        action="store_true",
        help="Only load GCPT coal metadata (gcpt_coal_metadata table)",
    )
    args = parser.parse_args()

    mutually_exclusive = sum(
        [
            args.schema_only,
            args.data_only,
            args.test_only,
            args.generator_info_only,
            args.gcpt_only,
        ]
    )
    if mutually_exclusive > 1:
        print(
            "ERROR: --schema-only, --data-only, --test-only, --generator-info-only, and --gcpt-only are mutually exclusive"
        )
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

    if args.gcpt_only:
        print("Loading GCPT coal metadata...")
        load_gcpt_coal_metadata(engine)
        print()
    elif args.generator_info_only:
        print("Loading EIA Form 860 generator info...")
        load_eia_generator_info(engine)
        print()
    elif args.test_only:
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

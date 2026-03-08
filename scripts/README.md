# Scripts

One-time setup scripts for bootstrapping the Neon DB with reference data. These are run manually, not by the dashboard or the unified crosswalk pipeline.

## Run Order

```
build_gcpt_crosswalks.py  -->  bootstrap_neon_db.py  -->  (Neon DB ready)
```

1. **`build_gcpt_crosswalks.py`** reads local GCPT Excel/CSV files and produces per-source crosswalk parquets (`data/crosswalks/{eia,entsoe,npp}_plant_coordinates.parquet`).
2. **`bootstrap_neon_db.py`** loads those parquets plus GPPD and schema SQL into the Neon PostgreSQL database.

Once the DB is populated, `src/build_crosswalk.py` can run the unified matching pipeline.

## Usage

```bash
# Step 1: Build crosswalk parquets from GCPT data
uv run python scripts/build_gcpt_crosswalks.py --source all

# Step 2: Load schema + reference data into Neon
uv run python scripts/bootstrap_neon_db.py

# Options
uv run python scripts/bootstrap_neon_db.py --schema-only   # Schema only, skip data
uv run python scripts/bootstrap_neon_db.py --data-only     # Data only, skip schema
uv run python scripts/bootstrap_neon_db.py --skip-gcpt     # Skip large GCPT table
```

## Prerequisites

- `.env` file with Neon DB credentials (copy from `.env.template`)
- GCPT Excel file in `data/gcpt/`
- ETL repo checked out at `../../etl/power-generation-etl/` (for schema SQL files)

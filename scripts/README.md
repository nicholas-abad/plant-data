# Scripts

One-time setup scripts for bootstrapping the Neon DB with reference data. These are run manually, not by the dashboard or the unified crosswalk pipeline.

## Run Order

```
build_gcpt_crosswalks.py  -->  bootstrap_neon_db.py  -->  (Neon DB ready)
```

1. **`build_gcpt_crosswalks.py`** reads local GCPT Excel/CSV files and produces per-source crosswalk parquets (`data/crosswalks/{eia,entsoe,npp}_plant_coordinates.parquet`).
2. **`bootstrap_neon_db.py`** loads `unified_plant_crosswalk.parquet` into `plant_crosswalk`, plus `eia_generator_info` (EIA Form 860) and `gcpt_coal_metadata` into the Neon PostgreSQL database.

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
uv run python scripts/bootstrap_neon_db.py --gcpt-only     # Skip large GCPT table
```

## Prerequisites

- `.env` file with Neon DB credentials (copy from `.env.template`)
- GCPT Excel file in `data/gcpt/`
- ETL repo checked out at `../../etl/power-generation-etl/` (for schema SQL files)

## The actual crosswalk workflow

```bash
uv run python -m src.build_crosswalk --sources ENTSOE --yes   # 1. build
uv run python scripts/verify_crosswalk.py                     # 2. GATE (non-zero = stop)
uv run python scripts/bootstrap_neon_db.py --data-only        # 3. swap into Neon
```

| Script | Role |
|---|---|
| `verify_crosswalk.py` | Regression gates on a freshly built parquet. Run **before** every production swap. |
| `fetch_hjks_units.py` | Refresh `data/crosswalks/hjks_units.csv` (Japanese unit rated outputs). Commit the result. |
| `bootstrap_neon_db.py` | Load reference data into Neon. Flags: `--schema-only`, `--data-only`, `--generator-info-only`, `--gcpt-only`, `--test-only`. |
| `build_gcpt_crosswalks.py` | **Legacy.** Produces per-source parquets nothing downstream consumes any more. |
| `test_npp_llm.py` | Spot-check LLM matching on India names. |
| `drop_old_reference_tables.sql` | One-off cleanup of retired reference tables. |

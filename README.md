# Plant Data Repository

Centralized plant coordinate matching for the energy generation dashboard. Maps plant names from 5 generation data sources (EIA, ENTSOE, NPP, ONS, OE) to geographic coordinates using a multi-stage matching pipeline: exact lookup, rapidfuzz, and LLM fallback.

## Reference Databases

| Source | Coverage | Location |
|--------|----------|----------|
| [GEM](https://globalenergymonitor.org/) (Global Energy Monitor) | Global coal/gas/etc plants | `data/GEM database_21Feb2026.csv` |
| GCPT (EIA crosswalk) | US coal plants with EIA IDs | `data/gcpt/*.xlsx` |
| [GPPD](https://datasets.wri.org/dataset/globalpowerplantdatabase) (Global Power Plant Database) | Global power plants | `data/crosswalks/global_power_plant_database.csv` |

## Repository Structure

```
plant-data/
├── src/
│   ├── build_crosswalk.py           # Unified crosswalk pipeline (main entry point)
│   ├── gcpt_loader.py               # Load GCPT Excel/CSV data
│   ├── utils.py                     # Path helpers, parquet I/O, validation
│   └── plant_name_matchers/
│       ├── base.py                  # BaseNameMatcher ABC + MatchResult
│       ├── gemini.py                # Google Gemini LLM implementation
│       ├── normalizers.py           # Shared name normalization functions
│       └── retriever.py             # CandidateRetriever for LLM prompts
├── scripts/
│   ├── build_gcpt_crosswalks.py     # GCPT Excel → per-source crosswalk parquets
│   └── bootstrap_neon_db.py         # Load schema + reference data into Neon DB
├── notebooks/
│   ├── npp_coordinate_coverage.ipynb # India NPP coverage analysis
│   └── eia_gem_coverage.ipynb        # US EIA coverage analysis
├── tests/
├── data/
│   ├── GEM database_21Feb2026.csv   # GEM reference database (~64MB)
│   ├── gcpt/                        # GCPT Excel files + README
│   ├── cache/                       # Runtime parquet caches (gitignored)
│   └── crosswalks/                  # Output: built crosswalk files
└── .env.template                    # Required: Neon DB + Gemini API credentials
```

## Installation

```bash
uv sync

# Or with pip
pip install -e .
```

Copy `.env.template` to `.env` and fill in your Neon DB credentials and Gemini API key.

## Unified Crosswalk Pipeline

The main entry point is `build_crosswalk.py`, which produces a single `unified_plant_crosswalk.parquet` mapping every plant to coordinates:

```bash
# Full pipeline (exact + rapidfuzz + LLM)
uv run python -m src.build_crosswalk

# Skip LLM step (faster, no API key needed)
uv run python -m src.build_crosswalk --no-llm

# Force rebuild (delete cached output)
uv run python -m src.build_crosswalk --force
```

### Pipeline Steps

1. **Pull plant names** from 5 Neon DB generation tables (NPP, ENTSOE, EIA, ONS, OE)
2. **Exact matching** -- EIA via GCPT crosswalk ID, OE via API-embedded coords
3. **Rapidfuzz matching** -- GEM (`token_sort_ratio >= 80`), GPPD (`token_sort_ratio >= 80`)
4. **LLM matching** -- Gemini API with top-15 candidates per source (high/medium confidence only)
5. **Output** -- `data/crosswalks/unified_plant_crosswalk.parquet`

### Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `plant_name` | str | Original plant name from generation data |
| `source_system` | str | NPP, ENTSOE, EIA, ONS, OE |
| `latitude` | float | Resolved latitude (null if unmatched) |
| `longitude` | float | Resolved longitude (null if unmatched) |
| `ref_source` | str | GEM, GPPD, OE-direct |
| `matching_method` | str | exact, rapidfuzz, llm, direct |
| `confidence` | str | high/medium/low (LLM only) |
| `ref_matched_name` | str | Name in reference DB that was matched |

## Other Scripts

```bash
# Build per-source crosswalk parquets from GCPT Excel data
uv run python scripts/build_gcpt_crosswalks.py --source all

# Bootstrap Neon DB with schema + reference data
uv run python scripts/bootstrap_neon_db.py
```

## License

- Code: MIT License
- GEM/GCPT Data: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)

Data attribution: Global Energy Monitor, Global Coal Plant Tracker, https://globalenergymonitor.org/projects/global-coal-plant-tracker/

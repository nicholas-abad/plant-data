# src/

Core library for plant coordinate matching.

## Modules

| File | Purpose |
|------|---------|
| `build_crosswalk.py` | **Main entry point.** Unified pipeline that matches plant names from all 5 generation sources to coordinates. Run with `python -m src.build_crosswalk`. |
| `gcpt_loader.py` | Loads Global Coal Plant Tracker (GCPT) Excel/CSV files. Used by `scripts/build_gcpt_crosswalks.py` to produce per-source crosswalk parquets. |
| `utils.py` | Path helpers (`get_data_dir`, `get_crosswalk_dir`), parquet I/O (`load_crosswalk`, `save_crosswalk`), coordinate validation. |
| `plant_name_matchers/` | Name normalization, fuzzy candidate retrieval, and LLM-based matching. See its own [README](plant_name_matchers/README.md). |

## How They Fit Together

```
gcpt_loader.py            Used by scripts/ to bootstrap Neon DB
utils.py                  Shared helpers used by build_crosswalk.py and scripts/
plant_name_matchers/      Normalization + LLM matching used by build_crosswalk.py
build_crosswalk.py        Queries Neon DB, runs matching pipeline, outputs parquet
```

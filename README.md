# Plant Data Repository

Centralized plant coordinate matching for the energy generation dashboard. Maps plant names from the generation data sources (EIA, ENTSOE, NPP, ONS, OE, OCCTO, Chile) to geographic coordinates using a multi-stage matching pipeline: exact lookup, rapidfuzz, and LLM fallback.

## Reference Databases

Everything below lives in **`data/crosswalks/`**. The large ones are gitignored — you must download them before a first build.

| Source | Provides | File | How to get it |
|---|---|---|---|
| [GEM](https://globalenergymonitor.org/) Global Integrated Power Tracker | Coordinates, coal type, combustion tech, capacity | `GEM database_21Feb2026.csv` | Request from GEM. **The filename is hardcoded** (`build_crosswalk.py`) — rename your download to match, or update the constant |
| [GPPD](https://datasets.wri.org/dataset/globalpowerplantdatabase) (WRI) | Coordinates, capacity (fallback when GEM misses) | `global_power_plant_database.csv` | Public download from WRI |
| [HJKS](https://hjks.jepx.or.jp/hjks/unit) (JEPX) | Japanese unit rated outputs (認可出力), keyed by 発電所コード | `hjks_units.csv` | **Committed** — refresh with `uv run python scripts/fetch_hjks_units.py` |
| NPP–GIPT crosswalk | India plant → GEM unit mapping | `NPP_GIPT_crosswalk (1).csv` | Curated by hand; note the literal `" (1)"` in the filename |
| EIA Form 860 | US generator metadata (`--generator-info-only`) | `3_1_Generator_Y2024.xlsx` | EIA Form 860 annual release |
| EIA plant lookup | plant_code → plant name | `eia_plant_lookup.csv` | Derived; see `notebooks/eia_plant_names.ipynb` |

> **Prerequisite:** the crosswalk build reads plant names **from the Neon database**, so the ETL must have loaded generation data first. Building against an empty database silently produces an empty crosswalk.

## Repository Structure

```
plant-data/
├── src/
│   ├── build_crosswalk.py           # THE pipeline (pull → match → capacity → parquet)
│   ├── gcpt_loader.py               # GCPT Excel/CSV loading (legacy path)
│   ├── utils.py                     # Path helpers, parquet I/O, coordinate validation
│   └── plant_name_matchers/         # normalizers, fuzzy retriever, Gemini LLM matcher
├── scripts/
│   ├── verify_crosswalk.py          # REGRESSION GATES — run before any prod swap
│   ├── bootstrap_neon_db.py         # Load the parquet + reference tables into Neon
│   ├── fetch_hjks_units.py          # Refresh the Japanese unit register
│   ├── build_gcpt_crosswalks.py     # Legacy GCPT → parquet (not in the main path)
│   ├── test_npp_llm.py              # LLM matching spot-check
│   └── drop_old_reference_tables.sql
├── notebooks/                       # eia_plant_names, llm_sanity_check, test_gemini_api
├── tests/                           # pytest — matching helpers, capacity logic, HJKS
├── data/crosswalks/                 # Reference inputs + the built parquet (see above)
└── .env.template                    # Neon DB credentials + GEMINI_API_KEY
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
5. **Capacity overrides** -- ENTSO-E site nameplate apportioned per unit by recent coal generation; OCCTO coal capacity from HJKS rated outputs joined by plant code (`capacity_source='HJKS'`)
6. **Output** -- `data/crosswalks/unified_plant_crosswalk.parquet` (gate it with `scripts/verify_crosswalk.py` before `scripts/bootstrap_neon_db.py --data-only`)

### Output Schema

`data/crosswalks/unified_plant_crosswalk.parquet` → loaded into Neon as `plant_crosswalk`. All 16 columns:

| Column | Description |
|---|---|
| `plant_name`, `plant_code` | Identity as the generation source spells it — this is the dashboard's join key |
| `source_system` | NPP, ENTSOE, EIA, ONS, OE, OCCTO, CHILE |
| `latitude`, `longitude` | Coordinates, or null when unmatched (the row is kept so its generation isn't lost) |
| `ref_source` | GEM, GPPD, or OE-direct |
| `matching_method` | direct, rapidfuzz, or llm |
| `confidence` | high/medium (LLM only; anything lower is discarded) |
| `ref_matched_name` | The name matched in the reference database |
| `reasoning` | The LLM's stated reason — the audit trail for a judgement call |
| `coal_type`, `combustion_tech` | Fuel metadata; drives the CO₂ emission factor downstream |
| `capacity_mw` | Operating coal capacity |
| `capacity_source` | Where a non-default capacity came from (`HJKS` for Japanese rated outputs); null = the `ref_source` figure |
| `state`, `sector` | Subregion and ownership, where the source provides them |

### How capacity is derived

The part most likely to break if you change something, so the rules are explicit:

- **Operating units only.** GEM lists every unit it has ever tracked, including *cancelled* (never built) and retired ones. Summing all statuses made Germany + Poland read ~145 GW against ~56 GW of operating plant — which is why European capacity factors were once a third of reality.
- **ENTSO-E candidates are restricted to the unit's own country.** A Europe-wide candidate pool once matched Kosovo's coal units to a German solar farm. `ENTSOE_AREA_COUNTRIES` maps all 37 TSO areas; an unmapped area fails the build loudly rather than silently widening the search.
- **Site nameplate is apportioned across units** by trailing-24-month coal generation, because ENTSO-E meters units while references track whole plants. Site and fleet totals stay exact; an individual unit's capacity is an estimate.
- **Japanese capacity comes from HJKS**, joined by plant code — no name matching involved.

### Verifying a rebuild

**Never swap a fresh crosswalk into production unverified.**

```bash
uv run python scripts/verify_crosswalk.py           # exits non-zero on any failure
uv run python scripts/bootstrap_neon_db.py --data-only
```

It compares against the git-committed previous parquet (`--baseline PATH` to override) and gates on: known-bad cross-border matches not recurring, every matched reference resolving inside its own country, capacity invariants and a plausible fleet total, fuel-aware NPP/OCCTO exemplars, and per-source coordinate coverage not dropping by more than 2 points.

### Gotchas

- **A rebuild overwrites manual DB fixes.** Anything hand-corrected in `plant_crosswalk` is replaced by the next `--data-only` swap. Fix causes in `build_crosswalk.py`, not rows in the table.
- **The build reads plant names from Neon**, so the ETL must have loaded generation data first — against an empty database you get an empty crosswalk with no error.
- **`--force` is ignored when `--sources` is given**; the incremental path merges into the existing parquet by design.
- **`--yes` is needed for non-interactive runs**, or the LLM cost prompt blocks. `--no-llm` skips Gemini entirely (no API key needed, lower coverage).

### GEM identity on `plant_crosswalk` (2026-08-30)

Every row carries its GEM link — or an explicit reason it has none:

| Column | Meaning |
|---|---|
| `gem_location_id` | The plant's permanent GEM identity (`L…`), or NULL |
| `gem_unit_id` | `G…` only where it comes for free (India's NPP-GIPT file names one unit) |
| `not_in_gem` | A person decided GEM has no record of this plant |
| `matching_method` | `direct` / `rapidfuzz` / `llm` (pipeline), `legacy` (today's matches grandfathered at cutover), `manual` (review team) |
| `decided_by`, `decided_on`, `note`, `override_reason` | Who decided, when, and why a cross-border link was allowed |
| `source_country` | GEM-named country of the source plant — the country guard compares against it |
| `candidate_1..3_{id,name,score}` | Pipeline hints for the reviewer; blank once decided |

**The one rule:** the weekly rebuild reads the live table's decided rows first (tier 0) and re-emits their link columns unchanged; it only fills rows that are still empty. Coordinates, capacity and coal type are re-derived from the GEM tables for every linked row (site capacity = operating coal units; Japan keeps HJKS ratings, Europe keeps per-unit apportionment).

**Review workflow** — no queue table, no admin page:

```bash
# 1. download:  Neon console → SQL editor → SELECT * FROM plant_crosswalk_review → Export CSV
# 2. fill:      gem_location_id (L…)  or  not_in_gem = true ; leave unknowns blank
# 3. upload:
uv run python scripts/import_decisions.py review.csv --by "C. Team"            # dry run
uv run python scripts/import_decisions.py review.csv --by "C. Team" --commit
```

The upload is **fill-empty-only**: rows already decided are skipped, so concurrent edits and stale files are harmless. The table's triggers refuse an unknown GEM ID or a wrong-country link (unless `--override-reason`), row by row, with reasons. `verify_crosswalk.py` gates 6a–6f check the same invariants on every build, plus that no prior decision was lost.

**Cutover (once):** `uv run python -m src.build_crosswalk --grandfather --force` keeps today's name-based GEM matches as `legacy` links so the dashboard does not move; the rows it cannot resolve to exactly one GEM location are the review team's first batch.

## Other Scripts

```bash
# Build per-source crosswalk parquets from GCPT Excel data
uv run python scripts/build_gcpt_crosswalks.py --source all

# Bootstrap Neon DB with schema + reference data
uv run python scripts/bootstrap_neon_db.py
```

## Bootstrap Database Reference Tables

The `bootstrap_neon_db.py` script loads reference tables into Neon PostgreSQL:

| Table | Source | Rows | Description |
|-------|--------|------|-------------|
| `plant_crosswalk` | Unified pipeline output | ~3,500 | Maps plant names to coordinates across all 7 sources |
| `eia_generator_info` | EIA Form 860 (3_1_Generator_Y2024.xlsx) | ~26,800 | Generator-level technology, prime mover, capacity |
| `gcpt_coal_metadata` | GCPT database | ~14,300 (1,170 USA with EIA IDs) | Coal type, combustion technology for CO2 estimation |

### CLI Flags

```bash
uv run python scripts/bootstrap_neon_db.py                    # Full bootstrap
uv run python scripts/bootstrap_neon_db.py --schema-only      # Schema only
uv run python scripts/bootstrap_neon_db.py --data-only        # All reference data
uv run python scripts/bootstrap_neon_db.py --generator-info-only  # EIA Form 860 only
uv run python scripts/bootstrap_neon_db.py --gcpt-only        # GCPT coal metadata only
uv run python scripts/bootstrap_neon_db.py --test-only        # NPP LLM test data only
```

### Schema Files

Located in `etl/power-generation-etl/schema/`:
- `eia_generator_info.sql` — EIA Form 860 generator reference data
- `gcpt_coal_metadata.sql` — GCPT coal type and technology for CO2 estimation

## GEM reference tables (`gem_locations`, `gem_units`, `gem_unit_status_snapshots`)

Global Energy Monitor's Ownership API is the **sole** reference for plant identity — no tracker spreadsheet is read anywhere in this pipeline. `scripts/fetch_gem.py` mirrors GEM's three power trackers (coal, gas & oil, bioenergy; ~14,300 locations, ~34,000 units) into Neon:

```bash
uv run python scripts/fetch_gem.py                 # weekly: core pull (<1 min); detail pull only when GEM released
uv run python scripts/fetch_gem.py --force-detail  # ≈ 40 min: coal type, technology, years, aliases, status history
uv run python scripts/fetch_gem.py --dry-run       # parquet to data/cache/gem/, Neon untouched
```

- **Core pull** (every run): `/assets?asset_type=…` paginated — id, location, name, status, capacity, country, coordinates for every unit.
- **Detail pull** (once per GEM release): `/assets/{L_G}` for coal units that are operating, mothballed or retired. Throttled to 4 requests/s, checkpointed in `data/cache/gem/` so it resumes, and guarded: if GEM publishes a new release mid-pull, the pull is discarded and repeated so a table never mixes releases. On a core-only run the previous detail columns are carried over.
- **Why all three trackers**: a third of the plants on the dashboard's map were located through GEM's non-coal records. Coordinates may come from any tracker; every coal aggregate downstream filters `tracker = 'GCPT'`.
- `gem_external_ids` is created empty: the API does not yet expose GEM's "Other IDs" column (EIA plant codes etc.). When it does, the loader fills it and ID-based US matching switches on — the deferred next item.
- Tables are swapped in with `_atomic_replace_table` (grants preserved). No foreign key `gem_units → gem_locations` on purpose: the swap's `DROP … CASCADE` would drop it; integrity is a post-condition of the pull and a `verify_crosswalk.py` gate.

Attribution: data © Global Energy Monitor, Global Coal Plant Tracker / Global Oil & Gas Plant Tracker / Global Bioenergy Power Tracker (release recorded per row in `gem_release`), CC BY 4.0.

## License

- Code: MIT License
- GEM/GCPT Data: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)

Data attribution: Global Energy Monitor, Global Coal Plant Tracker, https://globalenergymonitor.org/projects/global-coal-plant-tracker/

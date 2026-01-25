# Plant Data Repository

Centralized plant coordinate data for power generation analysis. This repository provides unified access to plant location data from the Global Coal Plant Tracker (GCPT) for use across all extractors (EIA, ENTSOE, India NPP) and the dashboard.

## Data Sources

| Source | Coverage | Format | License |
|--------|----------|--------|---------|
| [Global Coal Plant Tracker](https://globalenergymonitor.org/projects/global-coal-plant-tracker/) | Global coal plants ≥30MW | Excel | CC BY 4.0 |

## Repository Structure

```
plant-data/
├── data/
│   ├── gcpt/
│   │   ├── gcpt_global_2025.xlsx         # Full GCPT download
│   │   └── README.md                      # Data source, license, update date
│   └── crosswalks/
│       ├── eia_plant_coordinates.parquet  # Pre-computed EIA matches
│       ├── entsoe_plant_coordinates.parquet
│       └── npp_plant_coordinates.parquet
├── src/
│   ├── __init__.py
│   ├── gcpt_loader.py                     # Load and parse GCPT data
│   ├── coordinate_matcher.py              # Match plant names to coordinates
│   └── utils.py
├── scripts/
│   ├── download_gcpt.py                   # Download latest GCPT from GEM
│   └── build_crosswalks.py                # Generate source-specific crosswalks
├── tests/
│   └── test_coordinate_matcher.py
├── pyproject.toml
└── README.md
```

## Installation

```bash
# Install with uv
uv sync

# Or with pip
pip install -e .
```

## Usage

### Load GCPT Data

```python
from src.gcpt_loader import GCPTLoader

loader = GCPTLoader()
df = loader.load_global_data()
print(f"Loaded {len(df)} plant records")

# Filter by country
us_plants = loader.filter_by_country(["United States"])
eu_plants = loader.filter_by_country(["Germany", "France", "Poland"])
```

### Match Plant Names to Coordinates

```python
from src.coordinate_matcher import CoordinateMatcher

matcher = CoordinateMatcher()

# Fuzzy match plant names
results = matcher.match_plant_names(
    plant_names=["Colstrip", "Navajo Generating"],
    country="United States"
)
```

### Load Pre-computed Crosswalks

```python
import pandas as pd

# EIA crosswalk (exact matches on EIA plant + unit ID)
eia_coords = pd.read_parquet("data/crosswalks/eia_plant_coordinates.parquet")

# ENTSOE crosswalk (fuzzy matches on plant name + country)
entsoe_coords = pd.read_parquet("data/crosswalks/entsoe_plant_coordinates.parquet")
```

## Building Crosswalks

To regenerate the crosswalk files from source data:

```bash
# Build all crosswalks
uv run python scripts/build_crosswalks.py

# Build specific crosswalk
uv run python scripts/build_crosswalks.py --source eia
```

## Data Update Schedule

GCPT data is updated bi-annually (January/July). To update:

1. Download latest GCPT from [Global Energy Monitor](https://globalenergymonitor.org/projects/global-coal-plant-tracker/download-data/)
2. Place in `data/gcpt/gcpt_global_YYYY.xlsx`
3. Update `data/gcpt/README.md` with new version info
4. Run `scripts/build_crosswalks.py` to regenerate crosswalks

## License

- Code: MIT License
- GCPT Data: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)

Data attribution: Global Energy Monitor, Global Coal Plant Tracker, [release date], https://globalenergymonitor.org/projects/global-coal-plant-tracker/

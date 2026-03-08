#!/usr/bin/env python3
"""Build source-specific crosswalk files from GCPT data."""

import argparse
import sys
from pathlib import Path

import pandas as pd
from loguru import logger

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.gcpt_loader import GCPTLoader
from src.utils import save_crosswalk, get_crosswalk_dir


def build_eia_crosswalk(loader: GCPTLoader) -> pd.DataFrame:
    """
    Build EIA crosswalk from GCPT data.

    EIA crosswalk uses exact matches on EIA plant + unit ID field.

    Returns:
        DataFrame with EIA plant coordinates.
    """
    logger.info("Building EIA crosswalk...")

    df = loader.load_global_data()
    if df.empty:
        logger.error("No GCPT data loaded")
        return pd.DataFrame()

    # Filter to records with EIA IDs
    if "eia_plant_unit_id" not in df.columns:
        logger.error("eia_plant_unit_id column not found")
        return pd.DataFrame()

    eia_df = df[df["eia_plant_unit_id"].notna()].copy()

    # Parse EIA ID into plant_code and generator_id
    eia_df[["plant_code", "generator_id"]] = eia_df["eia_plant_unit_id"].str.split(
        "|", expand=True
    )

    # Select relevant columns
    columns = [
        "eia_plant_unit_id",
        "plant_code",
        "generator_id",
        "project_name",
        "unit_name",
        "latitude",
        "longitude",
        "subnational",
        "status",
    ]
    existing_cols = [c for c in columns if c in eia_df.columns]
    result = eia_df[existing_cols].copy()

    # Remove rows without valid coordinates
    result = result.dropna(subset=["latitude", "longitude"])

    logger.info(f"Built EIA crosswalk with {len(result)} records")
    return result


def build_entsoe_crosswalk(loader: GCPTLoader) -> pd.DataFrame:
    """
    Build ENTSOE crosswalk from GCPT data.

    ENTSOE crosswalk filters to European plants for fuzzy matching.

    Returns:
        DataFrame with European plant coordinates.
    """
    logger.info("Building ENTSOE crosswalk...")

    df = loader.get_european_plants()
    if df.empty:
        logger.warning("No European plants found in GCPT data")
        return pd.DataFrame()

    # Select relevant columns for matching
    columns = [
        "project_name",
        "unit_name",
        "country",
        "subnational",
        "latitude",
        "longitude",
        "status",
        "capacity_mw",
    ]
    existing_cols = [c for c in columns if c in df.columns]
    result = df[existing_cols].copy()

    # Remove rows without valid coordinates
    result = result.dropna(subset=["latitude", "longitude"])

    # Remove duplicates (keep first occurrence)
    result = result.drop_duplicates(subset=["project_name", "unit_name"], keep="first")

    logger.info(f"Built ENTSOE crosswalk with {len(result)} records")
    return result


def build_npp_crosswalk(loader: GCPTLoader) -> pd.DataFrame:
    """
    Build India NPP crosswalk from GCPT data.

    NPP crosswalk filters to Indian plants for fuzzy matching.

    Returns:
        DataFrame with Indian plant coordinates.
    """
    logger.info("Building India NPP crosswalk...")

    df = loader.get_indian_plants()
    if df.empty:
        logger.warning("No Indian plants found in GCPT data")
        return pd.DataFrame()

    # Select relevant columns for matching
    columns = [
        "project_name",
        "unit_name",
        "country",
        "subnational",
        "latitude",
        "longitude",
        "status",
        "capacity_mw",
    ]
    existing_cols = [c for c in columns if c in df.columns]
    result = df[existing_cols].copy()

    # Remove rows without valid coordinates
    result = result.dropna(subset=["latitude", "longitude"])

    # Remove duplicates (keep first occurrence)
    result = result.drop_duplicates(subset=["project_name", "unit_name"], keep="first")

    logger.info(f"Built India NPP crosswalk with {len(result)} records")
    return result


def main():
    """Main entry point for building crosswalks."""
    parser = argparse.ArgumentParser(
        description="Build source-specific crosswalk files from GCPT data"
    )
    parser.add_argument(
        "--source",
        choices=["eia", "entsoe", "npp", "all"],
        default="all",
        help="Which crosswalk to build (default: all)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory (default: data/crosswalks)",
    )
    args = parser.parse_args()

    # Configure logging
    logger.remove()
    logger.add(sys.stderr, level="INFO")

    # Initialize loader
    loader = GCPTLoader()

    # Determine output directory
    output_dir = args.output_dir or get_crosswalk_dir()
    output_dir.mkdir(parents=True, exist_ok=True)

    # Build requested crosswalks
    builders = {
        "eia": build_eia_crosswalk,
        "entsoe": build_entsoe_crosswalk,
        "npp": build_npp_crosswalk,
    }

    sources = list(builders.keys()) if args.source == "all" else [args.source]

    for source in sources:
        logger.info(f"Building {source} crosswalk...")
        df = builders[source](loader)

        if not df.empty:
            save_crosswalk(df, source)
            logger.info(f"Saved {source} crosswalk with {len(df)} records")
        else:
            logger.warning(f"No data for {source} crosswalk")

    logger.info("Crosswalk building complete")


if __name__ == "__main__":
    main()

"""Utility functions for plant data operations."""

from pathlib import Path
from typing import Optional

import pandas as pd
from loguru import logger


def get_package_root() -> Path:
    """Get the root directory of the plant-data package."""
    return Path(__file__).parent.parent


def get_data_dir() -> Path:
    """Get the data directory path."""
    return get_package_root() / "data"


def get_crosswalk_dir() -> Path:
    """Get the crosswalks directory path."""
    return get_data_dir() / "crosswalks"


def load_crosswalk(source: str) -> Optional[pd.DataFrame]:
    """
    Load a pre-computed crosswalk file.

    Args:
        source: Source name ("eia", "entsoe", or "npp").

    Returns:
        DataFrame with crosswalk data, or None if not found.
    """
    crosswalk_dir = get_crosswalk_dir()
    filename = f"{source}_plant_coordinates.parquet"
    filepath = crosswalk_dir / filename

    if not filepath.exists():
        logger.warning(f"Crosswalk file not found: {filepath}")
        return None

    try:
        df = pd.read_parquet(filepath)
        logger.info(f"Loaded {len(df)} records from {filename}")
        return df
    except Exception as e:
        logger.error(f"Error loading crosswalk {filename}: {e}")
        return None


def save_crosswalk(df: pd.DataFrame, source: str) -> bool:
    """
    Save a crosswalk DataFrame to parquet.

    Args:
        df: DataFrame to save.
        source: Source name for filename.

    Returns:
        True if successful, False otherwise.
    """
    crosswalk_dir = get_crosswalk_dir()
    crosswalk_dir.mkdir(parents=True, exist_ok=True)

    filename = f"{source}_plant_coordinates.parquet"
    filepath = crosswalk_dir / filename

    try:
        df.to_parquet(filepath, index=False)
        logger.info(f"Saved {len(df)} records to {filename}")
        return True
    except Exception as e:
        logger.error(f"Error saving crosswalk {filename}: {e}")
        return False


def parse_eia_id(eia_id: str) -> tuple[Optional[str], Optional[str]]:
    """
    Parse EIA plant+unit ID into components.

    Args:
        eia_id: EIA ID in format "plant_id|unit_id".

    Returns:
        Tuple of (plant_code, generator_id), or (None, None) if invalid.
    """
    if not eia_id or pd.isna(eia_id):
        return None, None

    parts = str(eia_id).split("|")
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()

    return None, None


def validate_coordinates(lat: float, lon: float) -> bool:
    """
    Validate that coordinates are within valid ranges.

    Args:
        lat: Latitude value.
        lon: Longitude value.

    Returns:
        True if valid, False otherwise.
    """
    if pd.isna(lat) or pd.isna(lon):
        return False

    return -90 <= lat <= 90 and -180 <= lon <= 180

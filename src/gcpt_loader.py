"""GCPT data loader for plant coordinates."""

from pathlib import Path
from typing import Optional

import pandas as pd
from loguru import logger


def _get_data_root() -> Path:
    """Get the data directory root (parent of src/)."""
    return Path(__file__).parent.parent / "data"


class GCPTLoader:
    """Loads and manages Global Coal Plant Tracker data."""

    # Standard column mappings for GCPT data
    COLUMN_MAPPING = {
        "Project Name": "project_name",
        "Unit Name": "unit_name",
        "Country/Area": "country",
        "Subnational": "subnational",
        "Latitude": "latitude",
        "Longitude": "longitude",
        "Status": "status",
        "Capacity (MW)": "capacity_mw",
        "EIA plant + unit ID": "eia_plant_unit_id",
    }

    def __init__(self, data_path: Optional[Path] = None):
        """
        Initialize the GCPT loader.

        Args:
            data_path: Optional path to GCPT data directory.
                       Defaults to data/gcpt/ relative to this module.
        """
        self._data_path = data_path or _get_data_root() / "gcpt"
        self._df: Optional[pd.DataFrame] = None

    def _find_gcpt_file(self) -> Optional[Path]:
        """
        Find the most recent GCPT data file.

        Returns:
            Path to the GCPT file or None if not found.
        """
        # Look for Excel files in the data directory
        excel_patterns = ["gcpt_global_*.xlsx", "GCPT*.xlsx", "*GCPT*.xlsx"]

        for pattern in excel_patterns:
            files = list(self._data_path.glob(pattern))
            if files:
                # Return the most recently modified file
                return max(files, key=lambda p: p.stat().st_mtime)

        # Also check for EIA crosswalk file (contains coordinate data)
        crosswalk_files = list(self._data_path.glob("*EIA*GCPT*crosswalk*.xlsx"))
        if crosswalk_files:
            return max(crosswalk_files, key=lambda p: p.stat().st_mtime)

        logger.warning(f"No GCPT file found in {self._data_path}")
        return None

    def load_global_data(self, force_reload: bool = False) -> pd.DataFrame:
        """
        Load the full GCPT dataset.

        Args:
            force_reload: Force reload from file even if already loaded.

        Returns:
            DataFrame with GCPT data.
        """
        if self._df is not None and not force_reload:
            return self._df

        gcpt_file = self._find_gcpt_file()
        if gcpt_file is None:
            logger.error("No GCPT file found")
            return pd.DataFrame()

        try:
            logger.info(f"Loading GCPT data from {gcpt_file}")
            df = pd.read_excel(gcpt_file)

            # Rename columns to standardized names (only rename if column exists)
            rename_map = {
                old: new for old, new in self.COLUMN_MAPPING.items()
                if old in df.columns
            }
            df = df.rename(columns=rename_map)

            # Ensure coordinate columns are numeric
            for col in ["latitude", "longitude"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            self._df = df
            logger.info(f"Loaded {len(df)} GCPT records")
            return df

        except Exception as e:
            logger.error(f"Error loading GCPT file: {e}")
            return pd.DataFrame()

    def filter_by_country(self, countries: list[str]) -> pd.DataFrame:
        """
        Filter GCPT data by country.

        Args:
            countries: List of country names to filter by.

        Returns:
            Filtered DataFrame.
        """
        df = self.load_global_data()
        if df.empty or "country" not in df.columns:
            return pd.DataFrame()

        return df[df["country"].isin(countries)].copy()

    def filter_by_status(self, statuses: list[str]) -> pd.DataFrame:
        """
        Filter GCPT data by plant status.

        Args:
            statuses: List of statuses (e.g., ["Operating", "Announced"]).

        Returns:
            Filtered DataFrame.
        """
        df = self.load_global_data()
        if df.empty or "status" not in df.columns:
            return pd.DataFrame()

        return df[df["status"].isin(statuses)].copy()

    def get_coordinates_for_plants(self, plant_ids: list[str]) -> pd.DataFrame:
        """
        Get coordinates for specific EIA plant+unit IDs.

        Args:
            plant_ids: List of EIA plant+unit IDs (format: "plant_id|unit_id").

        Returns:
            DataFrame with coordinates for matching plants.
        """
        df = self.load_global_data()
        if df.empty or "eia_plant_unit_id" not in df.columns:
            return pd.DataFrame()

        return df[df["eia_plant_unit_id"].isin(plant_ids)].copy()

    def get_us_plants(self) -> pd.DataFrame:
        """Get all US plants with EIA IDs."""
        return self.filter_by_country(["United States"])

    def get_european_plants(self) -> pd.DataFrame:
        """Get all European plants."""
        european_countries = [
            "Germany", "France", "Poland", "United Kingdom", "Italy",
            "Spain", "Netherlands", "Belgium", "Czech Republic", "Greece",
            "Romania", "Bulgaria", "Austria", "Hungary", "Slovakia",
            "Finland", "Denmark", "Portugal", "Ireland", "Slovenia",
            "Croatia", "Estonia", "Latvia", "Lithuania", "Luxembourg"
        ]
        return self.filter_by_country(european_countries)

    def get_indian_plants(self) -> pd.DataFrame:
        """Get all Indian plants."""
        return self.filter_by_country(["India"])

    @property
    def data(self) -> pd.DataFrame:
        """Get the loaded DataFrame (lazy load on first access)."""
        return self.load_global_data()

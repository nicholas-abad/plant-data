"""Coordinate matching utilities for plant names."""

from dataclasses import dataclass
from typing import Optional

import pandas as pd
from loguru import logger
from rapidfuzz import fuzz, process

from .gcpt_loader import GCPTLoader


@dataclass
class MatchResult:
    """Result of a plant name match."""
    query_name: str
    matched_name: str
    score: float
    latitude: Optional[float]
    longitude: Optional[float]
    country: Optional[str]
    unit_name: Optional[str]


class CoordinateMatcher:
    """Match plant names to coordinates using fuzzy matching."""

    def __init__(self, gcpt_loader: Optional[GCPTLoader] = None):
        """
        Initialize the coordinate matcher.

        Args:
            gcpt_loader: Optional GCPTLoader instance. Creates one if not provided.
        """
        self._loader = gcpt_loader or GCPTLoader()
        self._df: Optional[pd.DataFrame] = None

    def _get_data(self, country_filter: Optional[list[str]] = None) -> pd.DataFrame:
        """Get GCPT data with optional country filter."""
        if country_filter:
            return self._loader.filter_by_country(country_filter)
        return self._loader.load_global_data()

    def match_plant_names(
        self,
        plant_names: list[str],
        country: Optional[str] = None,
        countries: Optional[list[str]] = None,
        score_cutoff: float = 70.0,
        limit: int = 1,
    ) -> list[MatchResult]:
        """
        Match plant names to GCPT entries using fuzzy matching.

        Args:
            plant_names: List of plant names to match.
            country: Single country to filter by.
            countries: List of countries to filter by.
            score_cutoff: Minimum match score (0-100). Default 70.
            limit: Maximum matches per query. Default 1.

        Returns:
            List of MatchResult objects.
        """
        # Build country filter
        country_filter = None
        if country:
            country_filter = [country]
        elif countries:
            country_filter = countries

        df = self._get_data(country_filter)
        if df.empty:
            logger.warning("No GCPT data available for matching")
            return []

        # Get unique project names for matching
        if "project_name" not in df.columns:
            logger.error("project_name column not found in GCPT data")
            return []

        # Create choices from project names
        choices = df["project_name"].dropna().unique().tolist()

        results = []
        for name in plant_names:
            if not name or pd.isna(name):
                continue

            # Find best matches using rapidfuzz
            matches = process.extract(
                name,
                choices,
                scorer=fuzz.WRatio,
                score_cutoff=score_cutoff,
                limit=limit,
            )

            for matched_name, score, _ in matches:
                # Get the row(s) with this project name
                match_rows = df[df["project_name"] == matched_name]

                for _, row in match_rows.iterrows():
                    result = MatchResult(
                        query_name=name,
                        matched_name=matched_name,
                        score=score,
                        latitude=row.get("latitude"),
                        longitude=row.get("longitude"),
                        country=row.get("country"),
                        unit_name=row.get("unit_name"),
                    )
                    results.append(result)

        logger.info(f"Matched {len(results)} plant names from {len(plant_names)} queries")
        return results

    def match_eia_ids(
        self,
        eia_plant_unit_ids: list[str],
    ) -> pd.DataFrame:
        """
        Exact match EIA plant+unit IDs to coordinates.

        Args:
            eia_plant_unit_ids: List of EIA IDs in format "plant_id|unit_id".

        Returns:
            DataFrame with coordinates for matched IDs.
        """
        df = self._loader.load_global_data()
        if df.empty or "eia_plant_unit_id" not in df.columns:
            logger.warning("No EIA ID data available")
            return pd.DataFrame()

        matched = df[df["eia_plant_unit_id"].isin(eia_plant_unit_ids)].copy()
        logger.info(f"Matched {len(matched)} of {len(eia_plant_unit_ids)} EIA IDs")
        return matched

    def match_to_dataframe(
        self,
        source_df: pd.DataFrame,
        name_column: str,
        country_column: Optional[str] = None,
        score_cutoff: float = 70.0,
    ) -> pd.DataFrame:
        """
        Match a DataFrame of plant names to coordinates.

        Args:
            source_df: DataFrame with plant names.
            name_column: Column containing plant names.
            country_column: Optional column containing country names.
            score_cutoff: Minimum match score.

        Returns:
            Source DataFrame with added coordinate columns.
        """
        result_df = source_df.copy()
        result_df["gcpt_latitude"] = None
        result_df["gcpt_longitude"] = None
        result_df["gcpt_match_score"] = None
        result_df["gcpt_matched_name"] = None

        for idx, row in source_df.iterrows():
            name = row[name_column]
            country = row[country_column] if country_column else None

            matches = self.match_plant_names(
                [name],
                country=country,
                score_cutoff=score_cutoff,
                limit=1,
            )

            if matches:
                best = matches[0]
                result_df.at[idx, "gcpt_latitude"] = best.latitude
                result_df.at[idx, "gcpt_longitude"] = best.longitude
                result_df.at[idx, "gcpt_match_score"] = best.score
                result_df.at[idx, "gcpt_matched_name"] = best.matched_name

        matched_count = result_df["gcpt_latitude"].notna().sum()
        logger.info(f"Matched {matched_count}/{len(source_df)} rows to coordinates")

        return result_df

"""Tests for coordinate matcher functionality."""

import pytest
import pandas as pd
from unittest.mock import Mock, patch

from src.coordinate_matcher import CoordinateMatcher, MatchResult
from src.gcpt_loader import GCPTLoader


@pytest.fixture
def sample_gcpt_data():
    """Create sample GCPT data for testing."""
    return pd.DataFrame({
        "project_name": ["Colstrip", "Navajo Generating Station", "Scherer"],
        "unit_name": ["Unit 1", "Unit 1", "Unit 1"],
        "country": ["United States", "United States", "United States"],
        "subnational": ["Montana", "Arizona", "Georgia"],
        "latitude": [45.88, 36.91, 33.05],
        "longitude": [-106.61, -111.39, -83.77],
        "status": ["Operating", "Retired", "Operating"],
        "eia_plant_unit_id": ["6076|1", "4941|1", "6146|1"],
    })


@pytest.fixture
def mock_loader(sample_gcpt_data):
    """Create a mock GCPT loader."""
    loader = Mock(spec=GCPTLoader)
    loader.load_global_data.return_value = sample_gcpt_data
    loader.filter_by_country.return_value = sample_gcpt_data
    return loader


class TestCoordinateMatcher:
    """Tests for CoordinateMatcher class."""

    def test_match_plant_names_exact(self, mock_loader):
        """Test exact plant name matching."""
        matcher = CoordinateMatcher(gcpt_loader=mock_loader)

        results = matcher.match_plant_names(
            ["Colstrip"],
            country="United States",
            score_cutoff=90.0,
        )

        assert len(results) == 1
        assert results[0].matched_name == "Colstrip"
        assert results[0].latitude == 45.88
        assert results[0].longitude == -106.61

    def test_match_plant_names_fuzzy(self, mock_loader):
        """Test fuzzy plant name matching."""
        matcher = CoordinateMatcher(gcpt_loader=mock_loader)

        # "Navajo" should fuzzy match "Navajo Generating Station"
        results = matcher.match_plant_names(
            ["Navajo"],
            country="United States",
            score_cutoff=50.0,
        )

        assert len(results) == 1
        assert "Navajo" in results[0].matched_name

    def test_match_plant_names_no_match(self, mock_loader):
        """Test no match found."""
        matcher = CoordinateMatcher(gcpt_loader=mock_loader)

        results = matcher.match_plant_names(
            ["NonexistentPlant"],
            country="United States",
            score_cutoff=90.0,
        )

        assert len(results) == 0

    def test_match_eia_ids(self, mock_loader):
        """Test exact EIA ID matching."""
        matcher = CoordinateMatcher(gcpt_loader=mock_loader)

        results = matcher.match_eia_ids(["6076|1", "4941|1"])

        assert len(results) == 2
        assert "6076|1" in results["eia_plant_unit_id"].values
        assert "4941|1" in results["eia_plant_unit_id"].values

    def test_match_to_dataframe(self, mock_loader):
        """Test matching a DataFrame of plant names."""
        matcher = CoordinateMatcher(gcpt_loader=mock_loader)

        source_df = pd.DataFrame({
            "plant_name": ["Colstrip", "Scherer"],
            "state": ["MT", "GA"],
        })

        result = matcher.match_to_dataframe(
            source_df,
            name_column="plant_name",
            score_cutoff=90.0,
        )

        assert "gcpt_latitude" in result.columns
        assert "gcpt_longitude" in result.columns
        assert "gcpt_match_score" in result.columns
        assert result["gcpt_latitude"].notna().sum() == 2


class TestMatchResult:
    """Tests for MatchResult dataclass."""

    def test_match_result_creation(self):
        """Test MatchResult creation."""
        result = MatchResult(
            query_name="Test",
            matched_name="Test Plant",
            score=95.0,
            latitude=40.0,
            longitude=-75.0,
            country="United States",
            unit_name="Unit 1",
        )

        assert result.query_name == "Test"
        assert result.matched_name == "Test Plant"
        assert result.score == 95.0
        assert result.latitude == 40.0
        assert result.longitude == -75.0

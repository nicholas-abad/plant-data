"""Tests for the GEM-identity stages of the crosswalk build (no database)."""

import pandas as pd
import pytest

from src import build_crosswalk as bc
from src import gem_reference as gemref


@pytest.fixture
def gem(monkeypatch):
    """A tiny GEM reference: two US coal sites, one German, one alias."""
    locs = pd.DataFrame(
        {
            "gem_location_id": ["L1", "L2", "L3"],
            "name": ["Alpha power station", "Beta power station", "Gamma Kraftwerk"],
            "name_other": [None, "Beta Generating Station", None],
            "name_local": [None, None, None],
            "country": ["United States", "United States", "Germany"],
            "latitude": [10.0, 20.0, 50.0],
            "longitude": [-10.0, -20.0, 8.0],
        }
    ).set_index("gem_location_id", drop=False)
    units = pd.DataFrame(
        {
            "gem_unit_id": ["G1", "G2", "G3", "G4"],
            "gem_location_id": ["L1", "L1", "L2", "L3"],
            "tracker": ["GCPT", "GCPT", "GCPT", "GCPT"],
            "status": ["operating", "retired", "operating", "operating"],
            "capacity_mw": [500.0, 300.0, 700.0, 900.0],
            "coal_type": ["bituminous", "lignite", "waste coal", "lignite with CCS"],
            "combustion_tech": [
                "subcritical",
                "subcritical",
                "unknown",
                "supercritical/CCS",
            ],
        }
    )
    units["is_coal"] = True
    units["is_operating"] = units["status"] == "operating"
    monkeypatch.setattr(gemref, "_TABLES", {"locations": locs, "units": units})
    gemref._site_attrs.cache_clear()
    yield
    gemref._TABLES = None
    gemref._site_attrs.cache_clear()


class TestReference:
    def test_site_capacity_counts_operating_coal_only(self, gem):
        assert gemref.location("L1")["capacity_mw"] == 500.0
        assert gemref.location("L1")["coal_type"] == "bituminous"

    def test_vocabulary_normalised(self, gem):
        assert gemref.location("L2")["coal_type"] == "waste"
        assert gemref.location("L2")["combustion_tech"] is None  # 'unknown'
        assert gemref.location("L3")["coal_type"] == "lignite"
        assert gemref.location("L3")["combustion_tech"] == "supercritical"

    def test_name_index_has_aliases_and_ids(self, gem):
        idx = gemref.name_index(country="United States")
        assert idx["Beta Generating Station"]["gem_location_id"] == "L2"
        assert "Gamma Kraftwerk" not in idx

    def test_resolve_name_within_country(self, gem):
        assert gemref.resolve_name("beta generating station", "United States") == "L2"
        assert gemref.resolve_name("Alpha power station", "Germany") is None


def _rows(**over):
    base = {c: None for c in bc.OUTPUT_COLUMNS}
    df = pd.DataFrame(
        [
            {
                **base,
                "plant_name": "Alpha",
                "plant_code": "1",
                "source_system": "EIA",
                "matching_method": "rapidfuzz",
                "ref_source": "GEM",
                "ref_matched_name": "Alpha power station",
                "gem_location_id": "L1",
                "latitude": 10.0,
                "longitude": -10.0,
            },
            {**base, "plant_name": "Beta", "plant_code": "2", "source_system": "EIA"},
            {**base, "plant_name": "Gamma Kraftwerk A", "source_system": "ENTSOE"},
        ]
    )
    df["not_in_gem"] = False
    for k, v in over.items():
        df[k] = v
    return df


PLANTS = pd.DataFrame(
    {
        "plant_name": ["Alpha", "Beta", "Gamma Kraftwerk A"],
        "source_system": ["EIA", "EIA", "ENTSOE"],
        "ref_gem_country": [None, None, "Germany"],
        "ref_gppd_country": [None, None, "DEU"],
    }
)


class TestSourceCountry:
    def test_entsoe_from_area_others_from_config(self):
        rows = bc._stamp_source_country(_rows(), PLANTS)
        assert (
            rows.set_index("plant_name").loc["Gamma Kraftwerk A", "source_country"]
            == "Germany"
        )
        assert (
            rows.set_index("plant_name").loc["Alpha", "source_country"]
            == bc.SOURCE_COUNTRIES["EIA"]["gem"]
        )


class TestDecisions:
    def test_tier0_overrides_link_columns_by_natural_key(self):
        decisions = pd.DataFrame(
            [
                {
                    "source_system": "EIA",
                    "plant_code": "2",
                    "plant_name": "Beta",
                    "gem_location_id": "L2",
                    "gem_unit_id": None,
                    "not_in_gem": False,
                    "matching_method": "manual",
                    "decided_by": "C. Team",
                    "decided_on": "2026-09-01",
                    "note": None,
                    "override_reason": None,
                    "gem_name_at_decision": "Beta power station",
                    "gem_country_at_decision": "United States",
                }
            ]
        )
        rows = bc.apply_decisions(_rows(), decisions)
        beta = rows.set_index("plant_name").loc["Beta"]
        assert (
            beta.gem_location_id == "L2"
            and beta.matching_method == "manual"
            and beta.decided_by == "C. Team"
        )
        alpha = rows.set_index("plant_name").loc["Alpha"]
        assert (
            alpha.gem_location_id == "L1" and alpha.matching_method == "rapidfuzz"
        )  # untouched

    def test_derive_fills_decided_rows_from_gem(self, gem):
        rows = _rows()
        rows.loc[
            rows.plant_name == "Beta",
            ["gem_location_id", "decided_by", "matching_method"],
        ] = ["L2", "C. Team", "manual"]
        rows = bc.derive_from_gem(rows, {})
        beta = rows.set_index("plant_name").loc["Beta"]
        assert (beta.latitude, beta.longitude) == (20.0, -20.0)
        assert (
            beta.capacity_mw == 700.0
            and beta.coal_type == "waste"
            and beta.ref_source == "GEM"
        )
        alpha = rows.set_index("plant_name").loc["Alpha"]
        assert alpha.capacity_mw is None or pd.isna(
            alpha.capacity_mw
        )  # pipeline row not re-derived


class TestGrandfather:
    def test_live_name_match_becomes_legacy_link(self, gem):
        rows = bc._stamp_source_country(_rows(), PLANTS)
        live = pd.DataFrame(
            [
                {
                    "source_system": "EIA",
                    "plant_code": "2",
                    "plant_name": "Beta",
                    "ref_source": "GEM",
                    "ref_matched_name": "Beta Generating Station",
                    "matching_method": "llm",
                }
            ]
        )
        rows = bc.grandfather_legacy(rows, live)
        beta = rows.set_index("plant_name").loc["Beta"]
        assert (
            beta.gem_location_id == "L2"
            and beta.matching_method == "legacy"
            and beta.decided_by == "legacy-pipeline"
        )
        assert beta.gem_country_at_decision == "United States"

    def test_reproduced_match_stays_pipeline(self, gem):
        rows = bc._stamp_source_country(_rows(), PLANTS)
        live = pd.DataFrame(
            [
                {
                    "source_system": "EIA",
                    "plant_code": "1",
                    "plant_name": "Alpha",
                    "ref_source": "GEM",
                    "ref_matched_name": "Alpha power station",
                    "matching_method": "rapidfuzz",
                }
            ]
        )
        rows = bc.grandfather_legacy(rows, live)
        assert (
            rows.set_index("plant_name").loc["Alpha", "matching_method"] == "rapidfuzz"
        )

    def test_unresolvable_name_left_open(self, gem):
        rows = bc._stamp_source_country(_rows(), PLANTS)
        live = pd.DataFrame(
            [
                {
                    "source_system": "EIA",
                    "plant_code": "2",
                    "plant_name": "Beta",
                    "ref_source": "GEM",
                    "ref_matched_name": "Nowhere station",
                    "matching_method": "llm",
                }
            ]
        )
        rows = bc.grandfather_legacy(rows, live)
        assert pd.isna(rows.set_index("plant_name").loc["Beta", "gem_location_id"])


class TestCandidates:
    def test_open_rows_get_within_country_hints(self, gem):
        rows = bc._stamp_source_country(_rows(), PLANTS)
        rows = bc.add_candidates(rows, PLANTS)
        beta = rows.set_index("plant_name").loc["Beta"]
        assert beta.candidate_1_id == "L2"
        alpha = rows.set_index("plant_name").loc["Alpha"]
        assert alpha.candidate_1_id is None or pd.isna(
            alpha.candidate_1_id
        )  # already linked
        gamma = rows.set_index("plant_name").loc["Gamma Kraftwerk A"]
        assert gamma.candidate_1_id == "L3"  # German candidates only

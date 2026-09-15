"""Tests for the Climate TRACE lane of the crosswalk build (no database)."""

import pandas as pd
import pytest

from src import build_crosswalk as bc
from src import gem_reference as gemref


def _ct_row(**kw):
    base = dict(
        plant_code="1000001",
        plant_name="Alpha power station",
        iso3="USA",
        latitude=10.001,
        longitude=-10.001,
        ct_fuel="coal",
        capacity_mw=800.0,
    )
    base.update(kw)
    return base


@pytest.fixture
def gem(monkeypatch):
    """Tiny GEM reference: two US coal sites (one with an ambiguous twin),
    one German coal site, one US GAS site sharing Alpha's neighborhood."""
    locs = pd.DataFrame(
        {
            "gem_location_id": ["L1", "L2", "L2B", "L3", "LGAS"],
            "name": [
                "Alpha power station",
                "Beta power station",
                "Beta power station",  # ambiguous in-country name twin
                "Gamma Kraftwerk",
                "Delta CCGT",
            ],
            "name_other": [None, "Beta Generating Station", None, None, None],
            "name_local": [None, None, None, None, None],
            "country": [
                "United States",
                "United States",
                "United States",
                "Germany",
                "United States",
            ],
            "latitude": [10.0, 20.0, 21.0, 50.0, 30.0],
            "longitude": [-10.0, -20.0, -21.0, 8.0, -30.0],
        }
    ).set_index("gem_location_id", drop=False)
    units = pd.DataFrame(
        {
            "gem_unit_id": ["G1", "G2", "G2B", "G3"],
            "gem_location_id": ["L1", "L2", "L2B", "L3"],
            "tracker": ["GCPT"] * 4,
            "status": ["operating"] * 4,
            "capacity_mw": [500.0, 700.0, 650.0, 900.0],
            "coal_type": ["bituminous", "bituminous", "bituminous", "lignite"],
            "combustion_tech": ["subcritical"] * 4,
        }
    )
    # LGAS deliberately has NO GCPT units — it is not a coal site.
    units["is_coal"] = True
    units["is_operating"] = True
    monkeypatch.setattr(gemref, "_TABLES", {"locations": locs, "units": units})
    gemref._site_attrs.cache_clear()
    yield
    gemref._TABLES = None
    gemref._site_attrs.cache_clear()


class TestTierA:
    def test_exact_unique_near_links(self, gem):
        out = bc.match_ct(pd.DataFrame([_ct_row()]))
        r = out.iloc[0]
        assert r.gem_location_id == "L1"
        assert r.matching_method == "ct-name-geo"
        assert r.confidence == "high"
        assert r.ref_source == "GEM"
        # GEM identity: coords + coal attrs come from GEM…
        assert r.latitude == 10.0 and r.coal_type == "bituminous"
        # …but capacity stays CT's (co-fired plants; see D2 in the plan)
        assert r.capacity_mw == 800.0 and r.capacity_source == "CT"

    def test_exact_name_but_far_away_stays_open(self, gem):
        out = bc.match_ct(pd.DataFrame([_ct_row(latitude=11.0)]))  # ~111 km off
        r = out.iloc[0]
        assert pd.isna(r.gem_location_id)
        assert r.candidate_1_id == "L1"  # still the obvious suggestion
        # unlinked rows keep CT's own coordinates so the plant stays on the map
        assert r.latitude == 11.0 and r.capacity_mw == 800.0

    def test_ambiguous_name_on_site_resolved_by_geography(self, gem):
        # name twins L2/L2B: tier A refuses, but the plant sits ON L2 —
        # tier B's geography legitimately disambiguates
        out = bc.match_ct(
            pd.DataFrame(
                [
                    _ct_row(
                        plant_name="Beta power station",
                        latitude=20.001,
                        longitude=-20.001,
                    )
                ]
            )
        )
        r = out.iloc[0]
        assert r.gem_location_id == "L2"
        assert r.matching_method == "ct-geo-fuzzy"

    def test_ambiguous_name_far_from_both_goes_to_review(self, gem):
        out = bc.match_ct(
            pd.DataFrame(
                [
                    _ct_row(
                        plant_name="Beta power station", latitude=25.0, longitude=-25.0
                    )
                ]
            )
        )
        r = out.iloc[0]
        assert pd.isna(r.gem_location_id), (
            "ambiguous name with no geographic tiebreak must not link"
        )
        assert {r.candidate_1_id, r.candidate_2_id} == {"L2", "L2B"}

    def test_alias_matches(self, gem):
        out = bc.match_ct(
            pd.DataFrame(
                [
                    _ct_row(
                        plant_name="Beta Generating Station",
                        latitude=20.0,
                        longitude=-20.0,
                    )
                ]
            )
        )
        assert out.iloc[0].gem_location_id == "L2"


class TestTierB:
    def test_geo_first_with_fuzzy_name(self, gem):
        # name differs but is fuzzy-close, and the site is <2 km away
        out = bc.match_ct(
            pd.DataFrame(
                [
                    _ct_row(
                        plant_name="Alfa power station",
                        latitude=10.005,
                        longitude=-10.005,
                    )
                ]
            )
        )
        r = out.iloc[0]
        assert r.gem_location_id == "L1"
        assert r.matching_method == "ct-geo-fuzzy"
        assert r.confidence == "medium"

    def test_non_coal_neighbor_never_links(self, gem):
        # sits on top of the GAS site with a fuzzy-close name → must stay open
        out = bc.match_ct(
            pd.DataFrame(
                [
                    _ct_row(
                        plant_name="Delta CCGT plant",
                        latitude=30.001,
                        longitude=-30.001,
                    )
                ]
            )
        )
        assert pd.isna(out.iloc[0].gem_location_id)

    def test_unrelated_name_nearby_stays_open(self, gem):
        out = bc.match_ct(
            pd.DataFrame(
                [
                    _ct_row(
                        plant_name="Completely Different TPP",
                        latitude=10.005,
                        longitude=-10.005,
                    )
                ]
            )
        )
        assert pd.isna(out.iloc[0].gem_location_id)


class TestPlumbing:
    def test_unknown_iso3_fails_loudly(self, gem):
        with pytest.raises(ValueError, match="missing from CT_ISO3_TO_GEM_COUNTRY"):
            bc.match_ct(pd.DataFrame([_ct_row(iso3="ZZZ")]))

    def test_country_fence(self, gem):
        # exact name exists only in Germany; a US plant must not reach it
        out = bc.match_ct(
            pd.DataFrame(
                [_ct_row(plant_name="Gamma Kraftwerk", latitude=50.0, longitude=8.0)]
            )
        )
        assert pd.isna(out.iloc[0].gem_location_id)

    def test_duplicate_names_both_survive(self, gem):
        # two CT plants sharing a name (the 13-pairs case) — both rows out
        df = pd.DataFrame(
            [
                _ct_row(plant_code="1", plant_name="Twin plant", latitude=10.0),
                _ct_row(plant_code="2", plant_name="Twin plant", latitude=20.0),
            ]
        )
        out = bc.match_ct(df)
        assert len(out) == 2 and set(out.plant_code) == {"1", "2"}

    def test_source_country_stamp_preserves_ct(self, gem):
        out = bc.match_ct(pd.DataFrame([_ct_row()]))
        stamped = bc._stamp_source_country(
            out.copy(), pd.DataFrame({"source_system": []})
        )
        assert stamped.iloc[0].source_country == "United States"

    def test_map_values_exist_and_traps_spelled_right(self):
        m = bc.CT_ISO3_TO_GEM_COUNTRY
        assert m["TUR"] == "Türkiye"
        assert m["CZE"] == "Czech Republic"
        assert m["XKX"] == "Kosovo"
        assert m["MAC"] == "Macao"
        assert m["KOR"] == "South Korea"
        assert len(m) >= 86

    def test_output_columns_complete(self, gem):
        out = bc.match_ct(pd.DataFrame([_ct_row()]))
        assert list(out.columns) == bc.OUTPUT_COLUMNS
        assert bool(out.iloc[0].not_in_gem) is False

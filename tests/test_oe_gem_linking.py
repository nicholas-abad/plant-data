"""Tests for linking OE (Australia) coordinate-matched rows to GEM (no database)."""

import pandas as pd
import pytest

from src import build_crosswalk as bc
from src import gem_reference as gemref


@pytest.fixture
def gem(monkeypatch):
    """Tiny Australian GEM reference modelled on the real traps: a live coal
    station and a RETIRED demo stacked on the identical coordinate (Callide
    Oxyfuel), a retired coal site now running gas (Swanbank), a gas site."""
    locs = pd.DataFrame(
        {
            "gem_location_id": ["LOXY", "LCAL", "LBAY", "LSWB", "LGAS"],
            "name": [
                "Callide Oxyfuel Project",  # first in list order on purpose
                "Callide power station",
                "Bayswater power station",
                "Swanbank-E power station",
                "Pelican Point power station",
            ],
            "name_other": [None] * 5,
            "name_local": [None] * 5,
            "country": ["Australia"] * 5,
            "latitude": [-24.347, -24.347, -32.395, -27.660, -34.765],
            "longitude": [150.617, 150.617, 150.949, 152.813, 138.515],
        }
    ).set_index("gem_location_id", drop=False)
    units = pd.DataFrame(
        {
            "gem_unit_id": ["G1", "G2", "G3", "G4"],
            "gem_location_id": ["LBAY", "LCAL", "LOXY", "LSWB"],
            "tracker": ["GCPT"] * 4,
            "status": ["operating", "operating", "retired", "retired"],
            "capacity_mw": [2640.0, 1540.0, 30.0, 125.0],
            "coal_type": ["bituminous"] * 4,
            "combustion_tech": [
                "subcritical",
                "supercritical",
                "subcritical",
                "subcritical",
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


COAL = {"Bayswater", "Callide B", "Pelican Point"}


def _oe(name, lat, lon):
    row = {c: None for c in bc.OUTPUT_COLUMNS}
    row.update(
        plant_name=name,
        source_system="OE",
        latitude=lat,
        longitude=lon,
        ref_source="OE-direct",
        matching_method="direct",
        ref_matched_name=name,
    )
    return row


def test_exact_name_on_site_links_with_gem_identity(gem):
    out = bc.link_oe_to_gem(pd.DataFrame([_oe("Bayswater", -32.3951, 150.9492)]), COAL)
    r = out.iloc[0]
    assert r.gem_location_id == "LBAY"
    assert r.matching_method == "oe-name-geo" and r.confidence == "high"
    assert r.ref_source == "GEM" and r.ref_matched_name == "Bayswater power station"
    assert r.capacity_mw == 2640.0 and r.coal_type == "bituminous"


def test_same_name_far_away_stays_unlinked(gem):
    out = bc.link_oe_to_gem(pd.DataFrame([_oe("Bayswater", -30.0, 150.9)]), COAL)
    r = out.iloc[0]
    assert pd.isna(r.gem_location_id)
    assert r.matching_method == "direct" and r.latitude == -30.0


def test_unit_suffixed_name_links_to_live_station_not_dead_project(gem):
    # GEM stacks the live station and the retired demo on one point, and the
    # demo comes first in list order: the station must still win.
    out = bc.link_oe_to_gem(pd.DataFrame([_oe("Callide B", -24.346, 150.6186)]), COAL)
    r = out.iloc[0]
    assert r.gem_location_id == "LCAL"
    assert r.matching_method == "oe-geo-fuzzy"


def test_non_coal_site_is_never_linked(gem):
    out = bc.link_oe_to_gem(
        pd.DataFrame([_oe("Pelican Point", -34.765, 138.515)]), COAL
    )
    assert pd.isna(out.iloc[0].gem_location_id)


def test_gas_facility_on_retired_coal_site_is_not_linked(gem):
    # Swanbank E: exact name + on site, but OE reports it as gas; linking it
    # would displace the site's CT fallback with a feed the tracker ignores.
    out = bc.link_oe_to_gem(pd.DataFrame([_oe("Swanbank E", -27.655, 152.818)]), COAL)
    r = out.iloc[0]
    assert pd.isna(r.gem_location_id) and r.matching_method == "direct"


def test_missing_coordinates_left_alone(gem):
    out = bc.link_oe_to_gem(pd.DataFrame([_oe("Bayswater", None, None)]), COAL)
    assert pd.isna(out.iloc[0].gem_location_id)


def test_empty_input_passes_through(gem):
    empty = pd.DataFrame(columns=bc.OUTPUT_COLUMNS)
    assert bc.link_oe_to_gem(empty, COAL).empty


def test_non_coal_feed_pipeline_link_to_coal_site_is_vetoed(gem):
    def occto(name, loc, **kw):
        r = {c: None for c in bc.OUTPUT_COLUMNS}
        r.update(
            plant_name=name,
            source_system="OCCTO",
            gem_location_id=loc,
            ref_source="GEM",
            matching_method="rapidfuzz",
            latitude=1.0,
            longitude=1.0,
            capacity_mw=630.0,
        )
        r.update(kw)
        return r

    rows = pd.DataFrame(
        [
            occto("gas unit 2", "LBAY"),  # gas fuzzed onto coal site
            occto("coal unit", "LBAY"),  # genuine coal plant
            occto(
                "gas unit decided",
                "LBAY",
                decided_by="C. Team",
                matching_method="manual",
            ),
        ]
    )
    plants = pd.DataFrame(
        {
            "plant_name": ["gas unit 2", "coal unit", "gas unit decided"],
            "source_system": ["OCCTO"] * 3,
            "occto_has_coal": [False, True, False],
        }
    )
    npp_hydro = {c: None for c in bc.OUTPUT_COLUMNS}
    npp_hydro.update(
        plant_name="BHADRA HPS",
        source_system="NPP",
        gem_location_id="LBAY",
        ref_source="GEM",
        matching_method="rapidfuzz",
    )
    npp_coal = {**npp_hydro, "plant_name": "BAYSWATER TPS"}
    rows = pd.concat([rows, pd.DataFrame([npp_hydro, npp_coal])], ignore_index=True)
    plants = pd.concat(
        [
            plants,
            pd.DataFrame(
                {
                    "plant_name": ["BHADRA HPS", "BAYSWATER TPS"],
                    "source_system": ["NPP", "NPP"],
                    "npp_fuel": ["HYDRO", "THERMAL"],
                }
            ),
        ],
        ignore_index=True,
    )
    out = bc.veto_non_coal_feeds(rows, plants).set_index("plant_name")
    assert pd.isna(out.loc["BHADRA HPS", "gem_location_id"])
    assert out.loc["BAYSWATER TPS", "gem_location_id"] == "LBAY"
    assert pd.isna(out.loc["gas unit 2", "gem_location_id"])
    assert pd.isna(out.loc["gas unit 2", "capacity_mw"])
    assert out.loc["coal unit", "gem_location_id"] == "LBAY"
    assert out.loc["gas unit decided", "gem_location_id"] == "LBAY"

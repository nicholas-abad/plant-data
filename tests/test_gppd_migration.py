"""Tests for the one-off GPPD purge (migrate_gppd_to_gem)."""

import pandas as pd
import pytest

from src import build_crosswalk as bc
from src import gem_reference as gemref


@pytest.fixture
def gem(monkeypatch):
    locs = pd.DataFrame(
        {
            "gem_location_id": ["L1", "LGAS"],
            "name": ["Rawhide Energy Station", "Delta CCGT"],
            "name_other": [None, None],
            "name_local": [None, None],
            "country": ["United States", "United States"],
            "latitude": [40.0, 41.0],
            "longitude": [-105.0, -106.0],
        }
    ).set_index("gem_location_id", drop=False)
    units = pd.DataFrame(
        {
            "gem_unit_id": ["G1"],
            "gem_location_id": ["L1"],
            "tracker": ["GCPT"],
            "status": ["operating"],
            "capacity_mw": [280.0],
            "coal_type": ["subbituminous"],
            "combustion_tech": ["subcritical"],
        }
    )
    units["is_coal"] = True
    units["is_operating"] = True
    monkeypatch.setattr(gemref, "_TABLES", {"locations": locs, "units": units})
    gemref._site_attrs.cache_clear()
    yield
    gemref._TABLES = None
    gemref._site_attrs.cache_clear()


def _fresh_row(**kw):
    base = {c: None for c in bc.OUTPUT_COLUMNS}
    base.update(
        plant_name="Rawhide",
        plant_code="58122",
        source_system="EIA",
        source_country="United States",
        not_in_gem=False,
    )
    base.update(kw)
    return base


def _live_row(**kw):
    base = dict(
        plant_name="Rawhide",
        plant_code="58122",
        source_system="EIA",
        source_country="United States",
        ref_source="GPPD",
        ref_matched_name="Rawhide",
        latitude=40.0001,
        longitude=-105.0001,
        capacity_mw=270.0,
        decided_by=None,
    )
    base.update(kw)
    return base


class TestMigration:
    def test_geo_link_becomes_durable_decision(self, gem):
        rows = pd.DataFrame([_fresh_row()])
        out = bc.migrate_gppd_to_gem(rows, pd.DataFrame([_live_row()]))
        r = out.iloc[0]
        assert r.gem_location_id == "L1"
        assert r.matching_method == "gppd-geo"
        assert r.decided_by == "gppd-migration-2026-09"  # tier-0 durable
        assert r.ref_source == "GEM"
        assert r.latitude == 40.0 and r.coal_type == "subbituminous"
        # site capacity, capacity_source left NULL for downstream stamping /
        # ENTSO-E apportionment
        assert r.capacity_mw == 280.0 and r.capacity_source is None

    def test_unconfirmable_row_goes_open(self, gem):
        # live coords nowhere near any coal GEM site
        live = pd.DataFrame([_live_row(latitude=45.0, longitude=-100.0)])
        out = bc.migrate_gppd_to_gem(pd.DataFrame([_fresh_row()]), live)
        r = out.iloc[0]
        assert pd.isna(r.gem_location_id) and pd.isna(r.latitude)
        assert pd.isna(r.decided_by) and pd.isna(r.matching_method)
        assert "GPPD purge" in r.note

    def test_gas_neighbor_never_links(self, gem):
        live = pd.DataFrame(
            [_live_row(plant_name="Delta CCGT", latitude=41.0001, longitude=-106.0001)]
        )
        rows = pd.DataFrame([_fresh_row(plant_name="Delta CCGT")])
        out = bc.migrate_gppd_to_gem(rows, live)
        assert pd.isna(out.iloc[0].gem_location_id)

    def test_bad_name_near_site_stays_open(self, gem):
        live = pd.DataFrame([_live_row(plant_name="Totally Different", ref_matched_name="Nope")])
        rows = pd.DataFrame([_fresh_row(plant_name="Totally Different")])
        out = bc.migrate_gppd_to_gem(rows, live)
        assert pd.isna(out.iloc[0].gem_location_id)

    def test_gppd_name_probe_rescues_short_source_name(self, gem):
        # source name is weak but the GPPD-matched name equals GEM's
        live = pd.DataFrame(
            [_live_row(plant_name="RW1", ref_matched_name="Rawhide Energy Station")]
        )
        rows = pd.DataFrame([_fresh_row(plant_name="RW1")])
        out = bc.migrate_gppd_to_gem(rows, live)
        assert out.iloc[0].gem_location_id == "L1"

    def test_human_decision_untouched(self, gem):
        rows = pd.DataFrame([_fresh_row(decided_by="C. Team", gem_location_id="LGAS")])
        live = pd.DataFrame([_live_row()])
        out = bc.migrate_gppd_to_gem(rows, live)
        assert out.iloc[0].gem_location_id == "LGAS"
        assert out.iloc[0].decided_by == "C. Team"

    def test_non_gppd_rows_untouched(self, gem):
        rows = pd.DataFrame([_fresh_row(plant_name="Elsewhere", plant_code="999")])
        live = pd.DataFrame([_live_row()])  # keys don't match
        out = bc.migrate_gppd_to_gem(rows, live)
        assert pd.isna(out.iloc[0].gem_location_id)
        assert pd.isna(out.iloc[0].note)

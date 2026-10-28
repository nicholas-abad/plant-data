"""EIA plants linked by the EIA plant code GEM records (no database)."""

import sys
from pathlib import Path

import pandas as pd
import pytest

from src import build_crosswalk as bc
from src import eia_gem_ids as ids
from src import gem_reference as gemref
from src.gem_external_ids import eia_plant_locations, ids_from_export, parse_ids

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import verify_crosswalk as vc  # noqa: E402


@pytest.fixture
def gem(monkeypatch):
    """Coleto Creek (TX), Coal Creek (ND), a site with no state, one with two codes."""
    locs = pd.DataFrame(
        {
            "gem_location_id": ["L_COLETO", "L_COALCREEK", "L_NOSTATE", "L_A", "L_B"],
            "name": [
                "Coleto Creek power station",
                "Coal Creek Station",
                "Mill power station",
                "A",
                "B",
            ],
            "name_other": [None] * 5,
            "name_local": [None] * 5,
            "country": ["United States"] * 5,
            "state_province": ["Texas", "North Dakota", None, "Ohio", "Ohio"],
            "latitude": [28.7, 47.4, 30.0, 40.0, 40.1],
            "longitude": [-97.2, -101.2, -90.0, -82.0, -82.1],
        }
    ).set_index("gem_location_id", drop=False)
    units = pd.DataFrame(
        {
            "gem_unit_id": ["G1", "G2", "G3"],
            "gem_location_id": ["L_COLETO", "L_COALCREEK", "L_COALCREEK"],
            "tracker": ["GCPT"] * 3,
            "status": ["operating"] * 3,
            "capacity_mw": [600.0, 575.0, 575.0],
            "coal_type": ["subbituminous", "lignite", "lignite"],
            "combustion_tech": ["subcritical"] * 3,
        }
    )
    units["is_coal"] = True
    units["is_operating"] = True
    monkeypatch.setattr(gemref, "_TABLES", {"locations": locs, "units": units})
    gemref._site_attrs.cache_clear()
    yield
    gemref._TABLES = None
    gemref._site_attrs.cache_clear()


CODES = {
    "6178": {"L_COLETO"},
    "6030": {"L_COALCREEK"},
    "9001": {"L_NOSTATE"},
    "9002": {"L_A", "L_B"},
    "9003": {"L_GONE"},
}


class TestParse:
    def test_pairs_of_any_kind(self):
        assert parse_ids("Beyond Fossil Fuels: BE-25, ENTSO-E: 22WAMERCO000010Y") == [
            ("Beyond Fossil Fuels", "BE-25"),
            ("ENTSO-E", "22WAMERCO000010Y"),
        ]
        assert parse_ids("UDI ID (S&P Global Platts): 1, EIA: 2") == [
            ("UDI ID (S&P Global Platts)", "1"),
            ("EIA", "2"),
        ]
        assert parse_ids(None) == [] and parse_ids("") == []

    def test_export_gives_location_and_unit_ids(self, tmp_path):
        path = tmp_path / "export.csv"
        pd.DataFrame(
            {
                "Project ID": ["L1", "L1", "L2"],
                "Unit ID": ["G1", "G2", "G3"],
                "Non WEPP location IDs": [
                    "EIA: 6178, EIP: 4",
                    "EIA: 6178, EIP: 4",
                    "EIA: 1, EIA: 2",
                ],
                "Non WEPP unit IDs": ["EIA: 1", "EIA: 2", None],
            }
        ).to_csv(path, index=False)
        out = ids_from_export(path)
        loc = out[out["gem_unit_id"].isna()]
        assert sorted(map(tuple, loc[["gem_location_id", "namespace", "external_id"]].values)) == [
            ("L1", "EIA", "6178"), ("L1", "EIP", "4"), ("L2", "EIA", "1"), ("L2", "EIA", "2"),
        ]  # fmt: skip
        assert out[out["gem_unit_id"] == "G2"]["external_id"].tolist() == ["2"]
        assert eia_plant_locations(out) == {"6178": {"L1"}, "1": {"L2"}, "2": {"L2"}}

    def test_export_with_inconsistent_location_cells_is_refused(self, tmp_path):
        path = tmp_path / "export.csv"
        pd.DataFrame(
            {
                "Project ID": ["L1", "L1"],
                "Unit ID": ["G1", "G2"],
                "Non WEPP location IDs": ["EIA: 1", "EIA: 2"],
                "Non WEPP unit IDs": [None, None],
            }
        ).to_csv(path, index=False)
        with pytest.raises(ValueError, match="different location-ID cells"):
            ids_from_export(path)


class TestRule:
    def test_one_location_same_state_links(self, gem):
        assert ids.rule_target(CODES["6030"], "ND") == ids.Target("L_COALCREEK", "rule")

    @pytest.mark.parametrize(
        "code, states, reason",
        [
            ("6030", "TX", "state differs"),  # Coal Creek is not in Texas
            ("6030", "ND|TX", "EIA state unknown or several"),
            ("6030", None, "EIA state unknown or several"),
            ("9001", "LA", "GEM records no state"),
            ("9002", "OH", "GEM lists the code at 2 locations"),
            ("9003", "OH", "not in the gem_locations mirror"),
            ("0000", "OH", "no GEM code"),
        ],
    )
    def test_anything_doubtful_links_nowhere(self, gem, code, states, reason):
        t = ids.rule_target(CODES.get(code, set()), states)
        assert t.gem_location_id is None and reason in t.reason

    def test_review_verdicts_override_the_rule(self, gem):
        review = {
            "6030": {"verdict": "reject", "gem_location_id": "L_COALCREEK"},
            "9001": {"verdict": "accept", "gem_location_id": "L_NOSTATE"},
            "6178": {"verdict": "defer", "gem_location_id": "L_COLETO"},
        }
        assert ids.target("6030", CODES["6030"], "ND", review).gem_location_id is None
        assert ids.target("6178", CODES["6178"], "TX", review).gem_location_id is None
        assert ids.target("9001", CODES["9001"], "LA", review) == ids.Target(
            "L_NOSTATE", "review:accept"
        )

    def test_accepting_an_unknown_location_is_an_error(self, gem):
        with pytest.raises(ValueError, match="not in gem_locations"):
            ids.target(
                "1",
                set(),
                "OH",
                {"1": {"verdict": "accept", "gem_location_id": "L_GONE"}},
            )

    def test_review_file_is_validated(self, tmp_path):
        p = tmp_path / "review.csv"
        pd.DataFrame(
            {"plant_code": ["1"], "gem_location_id": ["L"], "verdict": ["maybe"]}
        ).to_csv(p, index=False)
        with pytest.raises(ValueError, match="unknown verdicts"):
            ids.load_review(p)
        pd.DataFrame(
            {
                "plant_code": ["1", "1"],
                "gem_location_id": ["L", "L"],
                "verdict": ["accept"] * 2,
            }
        ).to_csv(p, index=False)
        with pytest.raises(ValueError, match="listed twice"):
            ids.load_review(p)


class TestBuilderRows:
    def test_coal_creek_goes_to_coal_creek_and_attributes_derive(self, gem):
        plants = pd.DataFrame(
            {
                "plant_code": ["6030", "6178", "9002"],
                "plant_name": ["Coal Creek", "Coleto Creek", "Twin"],
                "eia_state": ["ND", "TX", "OH"],
            }
        )
        rows = ids.match_eia_by_gem_id(plants, CODES, {}, bc.OUTPUT_COLUMNS)
        assert dict(zip(rows["plant_code"], rows["gem_location_id"])) == {
            "6030": "L_COALCREEK",
            "6178": "L_COLETO",
        }
        assert set(rows["matching_method"]) == {ids.METHOD}
        rows["not_in_gem"] = False
        out = bc.derive_from_gem(rows, {})
        cc = out[out["plant_code"] == "6030"].iloc[0]
        assert (cc["latitude"], cc["capacity_mw"], cc["coal_type"]) == (
            47.4,
            1150.0,
            "lignite",
        )


class TestManifestGate:
    def test_only_exact_manifest_changes_pass(self, tmp_path, monkeypatch):
        mdir = tmp_path / "decision_migrations"
        mdir.mkdir()
        pd.DataFrame(
            [
                {"source_system": "EIA", "key": "6055", "old_gem_location_id": "L_BC1",
                 "old_decided_by": "legacy-pipeline", "gem_location_id": "L_BC2",
                 "decided_by": "gem-eia-id-2026-10", "note": "n"},
                {"source_system": "EIA", "key": "470", "old_gem_location_id": "",
                 "old_decided_by": "legacy-pipeline", "gem_location_id": "L_COM",
                 "decided_by": "gem-eia-id-2026-10", "note": "n"},
            ]
        ).to_csv(mdir / "m.csv", index=False)  # fmt: skip
        monkeypatch.setattr(ids, "MIGRATIONS_DIR", mdir)
        base = pd.DataFrame(
            {
                "gem_location_id": ["L_BC1", None, "L_X"],
                "decided_by": ["legacy-pipeline", "legacy-pipeline", "manual-team"],
            },
            index=["EIA|6055", "EIA|470", "EIA|1"],
        )
        now = pd.DataFrame(
            {
                "gem_location_id": ["L_BC2", "L_OTHER", "L_Y"],
                "decided_by": [
                    "gem-eia-id-2026-10",
                    "gem-eia-id-2026-10",
                    "manual-team",
                ],
            },
            index=base.index,
        )
        # 470 went somewhere the manifest does not say; 1 is not in it at all
        assert vc._migrated_decisions(base, now, list(base.index)) == {"EIA|6055"}


class TestStateVeto:
    def test_name_match_across_a_state_line_is_dropped(self, gem):
        rows = pd.DataFrame(
            {
                "source_system": ["EIA", "EIA", "EIA", "EIA"],
                "plant_code": ["6030", "6178", "1", "2"],
                "plant_name": ["Coal Creek", "Coleto Creek", "Kept by a person", "By code"],
                "gem_location_id": ["L_COLETO", "L_COLETO", "L_COLETO", "L_COLETO"],
                "matching_method": ["rapidfuzz", "rapidfuzz", "manual", ids.METHOD],
                "decided_by": [None, None, "team", None],
                "note": [None] * 4,
            }
        )
        for c in ["gem_unit_id", "ref_source", "ref_matched_name", "confidence",
                  "latitude", "longitude", "coal_type", "combustion_tech", "capacity_mw"]:  # fmt: skip
            rows[c] = None
        out = ids.veto_cross_state(rows, {"6030": "ND", "6178": "TX", "1": "ND", "2": "ND"})
        assert out["gem_location_id"].isna().tolist() == [True, False, False, False]
        assert "another state" in out.at[0, "note"]

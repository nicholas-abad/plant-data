"""Tests for the GEM API mirror (src/gem_api.py) on recorded API payloads."""

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from src.gem_api import (
    GemApiError,
    GemClient,
    apply_details,
    build_core_frames,
    carry_over_details,
    check_frames,
    detail_scope,
    parse_period,
    parse_release,
    releases_from_sources,
)

FIX = Path(__file__).parent / "fixtures" / "gem"
NOW = datetime(2026, 8, 30, tzinfo=timezone.utc)


def _load(name):
    return json.loads((FIX / name).read_text())


# -- releases -------------------------------------------------------------------


class TestReleases:
    def test_parse_release_month_year(self):
        assert (
            parse_release(
                "Copy-of-Global-Coal-Plant-Tracker-January-2026-DATA-TEAM-COPY.xlsx"
            )
            == "January 2026"
        )
        assert (
            parse_release(
                "Copy-of-Global-Oil-and-Gas-Plant-Tracker-GOGPT---January-2026-DATA-TEAM.xlsx"
            )
            == "January 2026"
        )

    def test_parse_release_version_fallback(self):
        assert (
            parse_release(
                "Copy-of-Global-Bioenergy-Power-Tracker-GBPT-V3-DATA-TEAM-COPY.xlsx"
            )
            == "V3"
        )

    def test_releases_from_real_sources(self):
        rel = releases_from_sources(_load("sources.json")["results"])
        assert rel == {"GCPT": "January 2026", "GOGPT": "January 2026", "GBPT": "V3"}

    def test_disagreeing_gogpt_files_is_an_error(self):
        rows = _load("sources.json")["results"]
        rows = [
            dict(r, source_file=r["source_file"].replace("January-2026", "April-2026"))
            if r["source_id"] == 5
            else r
            for r in rows
        ]
        with pytest.raises(GemApiError, match="disagree"):
            releases_from_sources(rows)

    def test_missing_tracker_is_an_error(self):
        rows = [
            r
            for r in _load("sources.json")["results"]
            if r["asset_type"] != "Bioenergy Plant"
        ]
        with pytest.raises(GemApiError, match="GBPT"):
            releases_from_sources(rows)


# -- core frames ----------------------------------------------------------------


def _core():
    page = _load("assets_page.json")["results"]
    gas = [
        dict(
            page[0],
            asset_id="L100000103878_G100009999999",
            asset_type="Oil & Gas Plant",
            unit_name="GT 1",
            operating_status="operating",
            capacity_value=100.0,
            project_name="A. B. Brown (gas)",
        )
    ]
    return build_core_frames(
        {"GCPT": page, "GOGPT": gas, "GBPT": []},
        {"GCPT": "January 2026", "GOGPT": "January 2026", "GBPT": "V3"},
        NOW,
    )


class TestCoreFrames:
    def test_locations_dedup_and_coal_name_wins(self):
        locs, units = _core()
        assert len(locs) == 1
        row = locs.iloc[0]
        assert row.gem_location_id == "L100000103878"
        assert (
            row["name"] == "A. B. Brown power station"
        )  # coal listed first, gas name ignored
        assert row.trackers == ["GCPT", "GOGPT"]
        assert row.gem_release == "GCPT:January 2026|GOGPT:January 2026"
        assert row.country == "United States"

    def test_units_carry_tracker_status_capacity(self):
        _, units = _core()
        assert len(units) == 3
        u = units.set_index("gem_unit_id")
        assert u.at["G100000100001", "tracker"] == "GCPT"
        assert u.at["G100000100001", "status"] == "retired"
        assert u.at["G100000100001", "capacity_mw"] == 265.2
        assert u.at["G100009999999", "tracker"] == "GOGPT"
        assert pd.isna(
            u.at["G100000100001", "coal_type"]
        )  # detail columns NULL before detail pull

    def test_bad_asset_id_rejected(self):
        with pytest.raises(GemApiError):
            build_core_frames(
                {
                    "GCPT": [{"asset_id": "X_Y", "country": "US"}],
                    "GOGPT": [],
                    "GBPT": [],
                },
                {"GCPT": "r", "GOGPT": "r", "GBPT": "r"},
                NOW,
            )

    def test_non_mw_capacity_rejected(self):
        page = _load("assets_page.json")["results"]
        page[0]["capacity_unit"] = "GW"
        with pytest.raises(GemApiError, match="capacity unit"):
            build_core_frames(
                {"GCPT": page, "GOGPT": [], "GBPT": []},
                {"GCPT": "r", "GOGPT": "r", "GBPT": "r"},
                NOW,
            )


# -- detail ---------------------------------------------------------------------


class TestDetail:
    def test_scope_is_coal_and_showable_statuses_only(self):
        _, units = _core()
        # gas unit excluded; both coal units are retired → in scope
        assert detail_scope(units) == [
            "L100000103878_G100000100001",
            "L100000103878_G100000100002",
        ]

    def test_parse_period(self):
        assert parse_period("H2 2014") == (2014, 2)
        assert parse_period("H1 2025") == (2025, 1)
        with pytest.raises(GemApiError):
            parse_period("Q3 2020")

    def test_apply_details_fills_unit_location_and_snapshots(self):
        locs, units = _core()
        detail = _load("asset_detail.json")
        units, locs, snaps = apply_details(
            units, locs, {detail["asset_id"]: detail}, "January 2026"
        )
        u = units.set_index("gem_unit_id").loc["G100000100001"]
        assert u.start_year == 1979 and u.retired_year == 2023
        assert u.retired_year_is_planned is False or u.retired_year_is_planned == False  # noqa: E712 (pandas boolean)
        assert u.coal_type == "bituminous" and u.combustion_tech == "subcritical"
        assert locs.iloc[0].name_local == "A.B. Brown Generating Station"
        assert len(snaps) == 6
        assert set(snaps.columns) == {
            "gem_unit_id",
            "period_year",
            "period_half",
            "status",
            "gem_release",
        }
        assert snaps.iloc[0].to_dict() == {
            "gem_unit_id": "G100000100001",
            "period_year": 2014,
            "period_half": 2,
            "status": "operating",
            "gem_release": "January 2026",
        }

    def test_planned_retirement_year_is_flagged(self):
        locs, units = _core()
        detail = _load("asset_detail.json")
        units.loc[units.gem_unit_id == "G100000100001", "status"] = "operating"
        detail["coal_plant_fields"]["retired_year"] = "2028.0"
        units, _, _ = apply_details(
            units, locs, {detail["asset_id"]: detail}, "January 2026"
        )
        u = units.set_index("gem_unit_id").loc["G100000100001"]
        assert u.retired_year == 2028 and bool(u.retired_year_is_planned) is True

    def test_carry_over_keeps_previous_details_on_core_only_run(self):
        locs, units = _core()
        detail = _load("asset_detail.json")
        prev_units, prev_locs, _ = apply_details(
            units.copy(), locs.copy(), {detail["asset_id"]: detail}, "January 2026"
        )
        fresh_locs, fresh_units = _core()  # detail columns NULL
        merged_units, merged_locs = carry_over_details(
            fresh_units, fresh_locs, prev_units, prev_locs
        )
        assert (
            merged_units.set_index("gem_unit_id").at["G100000100001", "coal_type"]
            == "bituminous"
        )
        assert merged_locs.iloc[0].name_local == "A.B. Brown Generating Station"


# -- post-conditions ------------------------------------------------------------


class TestCheckFrames:
    def test_small_mirror_fails_size_and_total_checks(self):
        locs, units = _core()
        problems = check_frames(
            locs, units, {"GCPT": 14509, "GOGPT": 14745, "GBPT": 4536}
        )
        assert any("locations" in p for p in problems)
        assert any(p.startswith("GCPT:") for p in problems)

    def test_orphan_and_mixed_release_detected(self):
        locs, units = _core()
        units.loc[0, "gem_location_id"] = "L999"
        units.loc[1, "gem_release"] = "April 2026"
        problems = check_frames(locs, units, {})
        assert any("not in gem_locations" in p for p in problems)
        assert any("mixed releases" in p for p in problems)


# -- client ---------------------------------------------------------------------


class _Resp:
    def __init__(self, status, payload=None, headers=None):
        self.status_code, self._payload, self.headers, self.text = (
            status,
            payload,
            headers or {},
            "",
        )

    def json(self):
        return self._payload


class _Session:
    def __init__(self, responses):
        self.responses, self.calls, self.headers = list(responses), [], {}

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, dict(params or {})))
        return self.responses.pop(0)


class TestClient:
    def test_pagination_follows_offset_and_stops_on_short_page(self):
        pages = [
            _Resp(200, {"total": 3, "results": [{"a": 1}, {"a": 2}]}),
            _Resp(200, {"total": 3, "results": [{"a": 3}]}),
        ]
        sess = _Session(pages)
        c = GemClient(session=sess, sleep=lambda s: None)
        assert [x["a"] for x in c.list_assets("coal-plant", limit=2)] == [1, 2, 3]
        assert [p["offset"] for _, p in sess.calls] == [0, 2]
        assert all(p["format"] == "json" for _, p in sess.calls)

    def test_retry_after_is_honoured_then_succeeds(self):
        slept = []
        sess = _Session(
            [_Resp(429, headers={"Retry-After": "3"}), _Resp(200, {"ok": True})]
        )
        c = GemClient(session=sess, sleep=slept.append, min_interval=0)
        assert c.get("/assets/x") == {"ok": True}
        assert 3.0 in slept

    def test_404_is_not_retried(self):
        sess = _Session([_Resp(404)])
        c = GemClient(session=sess, sleep=lambda s: None, min_interval=0)
        with pytest.raises(GemApiError, match="404"):
            c.get("/assets/nope")
        assert len(sess.calls) == 1

    def test_min_interval_paces_calls(self):
        slept = []
        sess = _Session([_Resp(200, {}), _Resp(200, {})])
        c = GemClient(session=sess, sleep=slept.append, min_interval=0.25)
        c.get("/a")
        c.get("/b")
        assert slept and 0 < slept[-1] <= 0.25

"""Global Energy Monitor (GEM) Ownership API → reference frames.

GEM's API is the SOLE reference source for plant identity (decision 2026-08-30):
no tracker spreadsheet is read anywhere in this pipeline. Open endpoints only —
no bulk download, no token. Everything here is server-side; the endpoint URLs
never reach the frontend (GEM's request).

Two pulls:

* **core pull** — every run, ~70 list pages, under a minute. Every unit of the
  three power trackers (coal, gas & oil, bioenergy) with id, location, name,
  status, capacity, country, coordinates. Enough for `gem_locations` and the
  core of `gem_units`.
* **detail pull** — once per GEM release, one call per coal unit that is
  operating, mothballed or retired (~9.9K, ~40 min at 4 req/s). Adds coal type,
  combustion technology, start/retired year, alternative names and the
  half-yearly status history. The list endpoint carries none of these.

Why all three trackers: a third of the plants on the dashboard's map were
located through GEM's gas/oil/bioenergy records (verified 2026-08-30), so a
coal-only mirror would empty most of Brazil. Coordinates may come from any
tracker; every coal aggregate downstream filters `tracker = 'GCPT'`.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Iterator

import pandas as pd
import requests
from loguru import logger

BASE_URL = "https://gem-ownership-api.fly.dev"
USER_AGENT = "coal-atlas-plant-data/1.0 (+https://github.com/nicholas-abad/plant-data)"

# API asset_type slug → tracker code used everywhere downstream.
TRACKERS: dict[str, str] = {
    "coal-plant": "GCPT",
    "oil-gas-plant": "GOGPT",
    "bioenergy-plant": "GBPT",
}
# /catalog/sources labels its rows by asset-type *label*, not slug.
SOURCE_LABEL_TO_TRACKER = {
    "Coal Plant": "GCPT",
    "Oil & Gas Plant": "GOGPT",
    "Bioenergy Plant": "GBPT",
}
# Statuses whose units the dashboard can ever show generation for. Cancelled,
# announced, shelved and under-construction units carry no generation and are
# excluded from the (expensive) detail pull.
# GEM's API status vocabulary is operating / retired / cancelled / planned;
# "mothballed" is a sub_status of operating and is therefore already covered.
DETAIL_STATUSES = {"operating", "retired"}

PAGE_LIMIT = 500  # API maximum


class GemApiError(RuntimeError):
    pass


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class GemClient:
    """Polite JSON client: fixed minimum interval, bounded retries, Retry-After."""

    def __init__(
        self,
        base_url: str = BASE_URL,
        min_interval: float = 0.25,
        timeout: float = 60.0,
        max_retries: int = 5,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.base_url = base_url.rstrip("/")
        self.min_interval = min_interval
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = session or requests.Session()
        self.session.headers.update(
            {"User-Agent": USER_AGENT, "Accept": "application/json"}
        )
        self._sleep = sleep
        self._last_call = 0.0
        self.calls = 0

    def get(self, path: str, **params: Any) -> Any:
        params.setdefault("format", "json")
        url = f"{self.base_url}/{path.lstrip('/')}"
        backoff = 1.0
        for attempt in range(1, self.max_retries + 1):
            wait = self.min_interval - (time.monotonic() - self._last_call)
            if wait > 0:
                self._sleep(wait)
            self._last_call = time.monotonic()
            self.calls += 1
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
            except requests.RequestException as e:
                if attempt == self.max_retries:
                    raise GemApiError(f"GET {path}: {e}") from e
                logger.warning(
                    f"GET {path}: {e} — retry {attempt}/{self.max_retries} in {backoff:.0f}s"
                )
                self._sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 404:
                raise GemApiError(f"GET {path}: 404 not found")
            if (
                resp.status_code in (429, 500, 502, 503, 504)
                and attempt < self.max_retries
            ):
                retry_after = resp.headers.get("Retry-After")
                delay = (
                    float(retry_after)
                    if retry_after and retry_after.isdigit()
                    else backoff
                )
                logger.warning(
                    f"GET {path}: HTTP {resp.status_code} — retry {attempt}/{self.max_retries} in {delay:.0f}s"
                )
                self._sleep(delay)
                backoff = min(backoff * 2, 30)
                continue
            raise GemApiError(f"GET {path}: HTTP {resp.status_code}: {resp.text[:200]}")
        raise GemApiError(f"GET {path}: retries exhausted")

    # -- endpoints ---------------------------------------------------------

    def list_assets(self, asset_type: str, limit: int = PAGE_LIMIT) -> Iterator[dict]:
        """Every list item for one asset type, following offset pagination."""
        offset = 0
        total: int | None = None
        while True:
            page = self.get(
                "/assets", asset_type=asset_type, limit=limit, offset=offset
            )
            results = page.get("results") or []
            total = page.get("total", total)
            yield from results
            offset += len(results)
            if (
                not results
                or len(results) < limit
                or (total is not None and offset >= total)
            ):
                return

    def asset_total(self, asset_type: str) -> int:
        return int(self.get("/assets", asset_type=asset_type, limit=1).get("total", 0))

    def asset(self, asset_id: str) -> dict:
        return self.get(f"/assets/{asset_id}")

    def sources(self) -> list[dict]:
        return self.get("/catalog/sources", limit=50).get("results") or []


# ---------------------------------------------------------------------------
# Releases
# ---------------------------------------------------------------------------

_MONTH_YEAR = re.compile(
    r"(January|February|March|April|May|June|July|August|September|October|November|December)[-_ ](\d{4})",
    re.IGNORECASE,
)


def parse_release(source_file: str) -> str:
    """'Copy-of-Global-Coal-Plant-Tracker-January-2026-DATA-TEAM-COPY.xlsx' → 'January 2026'.

    Falls back to a version token (e.g. 'GBPT-V3' → 'V3') and finally to the
    file stem, so a release string is never empty.
    """
    if not source_file or not source_file.strip():
        raise GemApiError(
            "empty source_file in /catalog/sources — cannot determine the GEM release"
        )
    m = _MONTH_YEAR.search(source_file)
    if m:
        return f"{m.group(1).capitalize()} {m.group(2)}"
    v = re.search(r"\bV(\d+)\b", source_file)
    if v:
        return f"V{v.group(1)}"
    return re.sub(r"\.xlsx?$", "", source_file)


def releases_from_sources(sources: Iterable[dict]) -> dict[str, str]:
    """tracker code → release string, from /catalog/sources.

    GOGPT is loaded from two files of the same release; the first wins and a
    disagreement is an error (it would mean a half-loaded API).
    """
    out: dict[str, str] = {}
    for row in sources:
        tracker = SOURCE_LABEL_TO_TRACKER.get(row.get("asset_type", ""))
        if not tracker:
            continue
        rel = parse_release(row.get("source_file", ""))
        if tracker in out and out[tracker] != rel:
            raise GemApiError(
                f"{tracker}: sources disagree on release ({out[tracker]!r} vs {rel!r})"
            )
        out.setdefault(tracker, rel)
    missing = set(TRACKERS.values()) - set(out)
    if missing:
        raise GemApiError(
            f"/catalog/sources has no row for tracker(s): {sorted(missing)}"
        )
    return out


# ---------------------------------------------------------------------------
# Core frames
# ---------------------------------------------------------------------------

LOCATION_COLUMNS = [
    "gem_location_id",
    "name",
    "name_other",
    "name_local",
    "country",
    "state_province",
    "latitude",
    "longitude",
    "location_accuracy",
    "wiki_url",
    "trackers",
    "gem_release",
    "fetched_at",
]
UNIT_COLUMNS = [
    "gem_unit_id",
    "gem_location_id",
    "tracker",
    "unit_name",
    "capacity_mw",
    "status",
    "sub_status",
    "start_year",
    "retired_year",
    "retired_year_is_planned",
    "planned_retirement",
    "coal_type",
    "combustion_tech",
    "gem_release",
    "fetched_at",
]
DETAIL_UNIT_COLUMNS = [
    "start_year",
    "retired_year",
    "retired_year_is_planned",
    "planned_retirement",
    "coal_type",
    "combustion_tech",
]
DETAIL_LOCATION_COLUMNS = ["name_other", "name_local", "location_accuracy"]
SNAPSHOT_COLUMNS = [
    "gem_unit_id",
    "period_year",
    "period_half",
    "status",
    "gem_release",
]


def split_asset_id(asset_id: str) -> tuple[str, str]:
    loc, _, unit = asset_id.partition("_")
    if not loc.startswith("L") or not unit.startswith("G"):
        raise GemApiError(f"unexpected asset_id {asset_id!r}")
    return loc, unit


def _capacity_mw(item: dict) -> float | None:
    v = item.get("capacity_value")
    if v is None:
        return None
    unit = (item.get("capacity_unit") or "MW").upper()
    if unit != "MW":
        raise GemApiError(
            f"{item.get('asset_id')}: capacity unit {unit!r}, expected MW"
        )
    return float(v)


def build_core_frames(
    items_by_tracker: dict[str, list[dict]],
    releases: dict[str, str],
    fetched_at: datetime,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """List items → (gem_locations, gem_units) with detail columns left NULL."""
    unit_rows: list[dict] = []
    loc_rows: dict[str, dict] = {}
    for tracker, items in items_by_tracker.items():
        rel = releases[tracker]
        for it in items:
            loc_id, unit_id = split_asset_id(it["asset_id"])
            if it.get("location_id") not in (None, loc_id):
                raise GemApiError(
                    f"{it['asset_id']}: location_id {it.get('location_id')} ≠ {loc_id}"
                )
            unit_rows.append(
                {
                    "gem_unit_id": unit_id,
                    "gem_location_id": loc_id,
                    "tracker": tracker,
                    "unit_name": it.get("unit_name"),
                    "capacity_mw": _capacity_mw(it),
                    "status": it.get("operating_status") or "unknown",
                    "sub_status": it.get("operating_sub_status"),
                    "start_year": None,
                    "retired_year": None,
                    "retired_year_is_planned": None,
                    "planned_retirement": None,
                    "coal_type": None,
                    "combustion_tech": None,
                    "gem_release": rel,
                    "fetched_at": fetched_at,
                }
            )
            loc = loc_rows.setdefault(
                loc_id,
                {
                    "gem_location_id": loc_id,
                    "name": None,
                    "name_other": None,
                    "name_local": None,
                    "country": None,
                    "state_province": None,
                    "latitude": None,
                    "longitude": None,
                    "location_accuracy": None,
                    "wiki_url": None,
                    "trackers": [],
                    "gem_release": None,
                    "fetched_at": fetched_at,
                    "_releases": {},
                },
            )
            # First non-null wins; coal tracker items are listed first so coal
            # names take precedence at multi-tracker sites.
            for src, dst in (
                ("project_name", "name"),
                ("country", "country"),
                ("state_province", "state_province"),
                ("latitude", "latitude"),
                ("longitude", "longitude"),
                ("wiki_url", "wiki_url"),
            ):
                if loc[dst] is None and it.get(src) is not None:
                    loc[dst] = it[src]
            if tracker not in loc["trackers"]:
                loc["trackers"].append(tracker)
            loc["_releases"][tracker] = rel
    for loc in loc_rows.values():
        loc["gem_release"] = "|".join(
            f"{t}:{loc['_releases'][t]}" for t in loc["trackers"]
        )
        del loc["_releases"]
        if loc["name"] is None:
            loc["name"] = loc["gem_location_id"]
        if loc["country"] is None:
            raise GemApiError(f"{loc['gem_location_id']}: no country on any unit")
    locations = pd.DataFrame(list(loc_rows.values()), columns=LOCATION_COLUMNS)
    units = pd.DataFrame(unit_rows, columns=UNIT_COLUMNS)
    if units["gem_unit_id"].duplicated().any():
        dups = (
            units.loc[units["gem_unit_id"].duplicated(), "gem_unit_id"].head(5).tolist()
        )
        raise GemApiError(f"duplicate unit ids across trackers: {dups}")
    return locations, units


# ---------------------------------------------------------------------------
# Detail pull
# ---------------------------------------------------------------------------


def detail_scope(units: pd.DataFrame) -> list[str]:
    """asset_ids (L_G) of coal units worth a detail call, deterministic order."""
    sel = units[(units["tracker"] == "GCPT") & (units["status"].isin(DETAIL_STATUSES))]
    return sorted(f"{r.gem_location_id}_{r.gem_unit_id}" for r in sel.itertuples())


def _year(v: Any) -> int | None:
    if v is None or v == "" or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


_PERIOD = re.compile(r"^H([12])\s+(\d{4})$")


def parse_period(period: str) -> tuple[int, int]:
    m = _PERIOD.match(period.strip())
    if not m:
        raise GemApiError(f"unparseable status period {period!r}")
    return int(m.group(2)), int(m.group(1))


def apply_details(
    units: pd.DataFrame,
    locations: pd.DataFrame,
    details: dict[str, dict],
    release: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Fold /assets/{id} payloads into the frames; return (units, locations, snapshots)."""
    units = units.set_index("gem_unit_id", drop=False)
    locations = locations.set_index("gem_location_id", drop=False)
    snap_rows: list[dict] = []
    for asset_id, d in details.items():
        _, unit_id = split_asset_id(asset_id)
        f = d.get("coal_plant_fields") or {}
        if unit_id not in units.index:
            logger.warning(f"detail for {asset_id} not in core pull — skipped")
            continue
        status = units.at[unit_id, "status"]
        ry = _year(f.get("retired_year"))
        units.at[unit_id, "start_year"] = _year(f.get("start_year"))
        units.at[unit_id, "retired_year"] = ry
        units.at[unit_id, "retired_year_is_planned"] = (
            (ry is not None and status != "retired") if ry is not None else None
        )
        units.at[unit_id, "planned_retirement"] = _year(f.get("planned_retirement"))
        units.at[unit_id, "coal_type"] = f.get("coal_type")
        units.at[unit_id, "combustion_tech"] = f.get("combustion_technology")
        loc_id = units.at[unit_id, "gem_location_id"]
        if loc_id in locations.index:
            for src, dst in (
                ("plant_name_2", "name_other"),
                ("plant_name_3", "name_local"),
                ("location_accuracy", "location_accuracy"),
            ):
                if pd.isna(locations.at[loc_id, dst]) and f.get(src):
                    locations.at[loc_id, dst] = f[src]
        for sc in d.get("status_changes") or []:
            year, half = parse_period(sc["period"])
            snap_rows.append(
                {
                    "gem_unit_id": unit_id,
                    "period_year": year,
                    "period_half": half,
                    "status": sc.get("value"),
                    "gem_release": release,
                }
            )
    for col in ("start_year", "retired_year", "planned_retirement"):
        units[col] = units[col].astype("Int64")
    units["retired_year_is_planned"] = units["retired_year_is_planned"].astype(
        "boolean"
    )
    snapshots = pd.DataFrame(snap_rows, columns=SNAPSHOT_COLUMNS)
    return units.reset_index(drop=True), locations.reset_index(drop=True), snapshots


def carry_over_details(
    units: pd.DataFrame,
    locations: pd.DataFrame,
    prev_units: pd.DataFrame,
    prev_locations: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Core-only run on an unchanged release: keep detail columns from the previous tables.

    Without this, every weekly core pull would wipe coal_type / years / aliases
    until the next quarterly detail pull.
    """
    if not prev_units.empty:
        prev = prev_units.set_index("gem_unit_id")[DETAIL_UNIT_COLUMNS]
        units = units.set_index("gem_unit_id")
        common = units.index.intersection(prev.index)
        units.loc[common, DETAIL_UNIT_COLUMNS] = prev.loc[
            common, DETAIL_UNIT_COLUMNS
        ].values
        units = units.reset_index()
    if not prev_locations.empty:
        prev = prev_locations.set_index("gem_location_id")[DETAIL_LOCATION_COLUMNS]
        locations = locations.set_index("gem_location_id")
        common = locations.index.intersection(prev.index)
        locations.loc[common, DETAIL_LOCATION_COLUMNS] = prev.loc[
            common, DETAIL_LOCATION_COLUMNS
        ].values
        locations = locations.reset_index()
    return units[UNIT_COLUMNS], locations[LOCATION_COLUMNS]


# ---------------------------------------------------------------------------
# Post-conditions
# ---------------------------------------------------------------------------


@dataclass
class PullSummary:
    releases: dict[str, str]
    api_totals: dict[str, int]
    n_locations: int
    n_units: int
    n_units_by_tracker: dict[str, int]
    n_snapshots: int
    detail_pulled: bool
    api_calls: int
    problems: list[str] = field(default_factory=list)


def check_frames(
    locations: pd.DataFrame,
    units: pd.DataFrame,
    api_totals: dict[str, int],
    tolerance: float = 0.02,
) -> list[str]:
    """Return a list of failed post-conditions (empty = OK)."""
    problems = []
    if len(locations) < 14_000:
        problems.append(f"only {len(locations):,} locations (expected ≥ 14,000)")
    for tracker, total in api_totals.items():
        n = int((units["tracker"] == tracker).sum())
        if total and abs(n - total) > tolerance * total:
            problems.append(
                f"{tracker}: {n:,} units pulled vs {total:,} reported by the API"
            )
    orphans = ~units["gem_location_id"].isin(locations["gem_location_id"])
    if orphans.any():
        problems.append(
            f"{int(orphans.sum())} units reference a location not in gem_locations"
        )
    for tracker, grp in units.groupby("tracker"):
        rels = grp["gem_release"].unique()
        if len(rels) != 1:
            problems.append(f"{tracker}: mixed releases {list(rels)}")
    if units["gem_unit_id"].duplicated().any():
        problems.append("duplicate gem_unit_id")
    if locations["gem_location_id"].duplicated().any():
        problems.append("duplicate gem_location_id")
    return problems


def now_utc() -> datetime:
    return datetime.now(timezone.utc)

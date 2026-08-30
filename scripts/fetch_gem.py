"""Mirror GEM's power-plant trackers into Neon: gem_locations, gem_units, gem_unit_status_snapshots.

Usage:
    uv run python scripts/fetch_gem.py                 # core pull; detail pull only if GEM released
    uv run python scripts/fetch_gem.py --force-detail  # detail pull regardless (≈ 40 min)
    uv run python scripts/fetch_gem.py --no-detail     # core pull only, carry previous details over
    uv run python scripts/fetch_gem.py --dry-run       # write parquet to data/cache/gem/, touch nothing in Neon
    uv run python scripts/fetch_gem.py --max-detail 20 # smoke test the detail path

Tables are swapped in with bootstrap_neon_db._atomic_replace_table (grants
preserved, empty frames refused). gem_external_ids is created empty and left
alone: the API does not expose GEM's "Other IDs" column yet (deferred item).

Release guard: /catalog/sources is read before and after the detail pull; if
GEM published a new release in between, the pull is discarded and repeated so a
table never mixes two releases. Detail responses are checkpointed to
data/cache/gem/details_<release>.jsonl so an interrupted pull resumes.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path

import pandas as pd
from loguru import logger
from sqlalchemy import text

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(SCRIPT_DIR))
sys.path.insert(0, str(ROOT))

from bootstrap_neon_db import _atomic_replace_table, get_engine  # noqa: E402
from src.gem_api import (  # noqa: E402
    LOCATION_COLUMNS,
    TRACKERS,
    UNIT_COLUMNS,
    GemApiError,
    GemClient,
    apply_details,
    build_core_frames,
    carry_over_details,
    check_frames,
    detail_scope,
    now_utc,
    releases_from_sources,
)

CACHE_DIR = ROOT / "data" / "cache" / "gem"

POST_LOAD_SQL = {
    "gem_locations": [
        "ALTER TABLE gem_locations ADD PRIMARY KEY (gem_location_id)",
        "CREATE INDEX idx_gem_locations_country ON gem_locations (country)",
    ],
    "gem_units": [
        "ALTER TABLE gem_units ADD PRIMARY KEY (gem_unit_id)",
        "CREATE INDEX idx_gem_units_location ON gem_units (gem_location_id)",
        "CREATE INDEX idx_gem_units_tracker_status ON gem_units (tracker, status)",
        # No FK to gem_locations on purpose: _atomic_replace_table swaps
        # gem_locations with DROP … CASCADE, which would drop the constraint.
        # Referential integrity is a post-condition in check_frames() and a
        # verify_crosswalk.py gate instead.
    ],
    "gem_unit_status_snapshots": [
        "ALTER TABLE gem_unit_status_snapshots ADD PRIMARY KEY (gem_unit_id, period_year, period_half)",
    ],
}

EXTERNAL_IDS_DDL = """
CREATE TABLE IF NOT EXISTS gem_external_ids (
    gem_location_id TEXT,
    gem_unit_id     TEXT,
    namespace       TEXT NOT NULL,
    external_id     TEXT NOT NULL
);
COMMENT ON TABLE gem_external_ids IS
  'Other systems'' IDs for GEM units/locations (EIA, AEMO DUID, …). Empty until the GEM API exposes the tracker''s "Other IDs" column.';
"""


def _slug(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "-", s).strip("-").lower()


def read_previous(engine) -> tuple[pd.DataFrame, pd.DataFrame, str | None]:
    """Existing gem_units / gem_locations (empty frames if absent) and the stored GCPT release."""
    with engine.connect() as conn:
        exists = {
            r[0]
            for r in conn.execute(
                text(
                    "SELECT relname FROM pg_class WHERE relnamespace = 'public'::regnamespace "
                    "AND relname IN ('gem_units', 'gem_locations')"
                )
            )
        }
        units = (
            pd.read_sql("SELECT * FROM gem_units", conn)
            if "gem_units" in exists
            else pd.DataFrame(columns=UNIT_COLUMNS)
        )
        locs = (
            pd.read_sql("SELECT * FROM gem_locations", conn)
            if "gem_locations" in exists
            else pd.DataFrame(columns=LOCATION_COLUMNS)
        )
    rel = None
    if not units.empty:
        gcpt = units.loc[units["tracker"] == "GCPT", "gem_release"].dropna().unique()
        rel = str(gcpt[0]) if len(gcpt) == 1 else None
    return units, locs, rel


def load_checkpoint(path: Path) -> dict[str, dict]:
    out: dict[str, dict] = {}
    if path.exists():
        with path.open() as fh:
            for line in fh:
                line = line.strip()
                if line:
                    rec = json.loads(line)
                    out[rec["asset_id"]] = rec["payload"]
    return out


def run_detail_pull(
    client: GemClient, asset_ids: list[str], checkpoint: Path, max_units: int | None
) -> dict[str, dict]:
    details = load_checkpoint(checkpoint)
    todo = [a for a in asset_ids if a not in details]
    if max_units is not None:
        todo = todo[:max_units]
    logger.info(f"detail pull: {len(details):,} cached, {len(todo):,} to fetch")
    checkpoint.parent.mkdir(parents=True, exist_ok=True)
    t0 = time.monotonic()
    with checkpoint.open("a") as fh:
        for i, asset_id in enumerate(todo, 1):
            payload = client.asset(asset_id)
            details[asset_id] = payload
            fh.write(json.dumps({"asset_id": asset_id, "payload": payload}) + "\n")
            if i % 200 == 0:
                fh.flush()
                rate = i / max(time.monotonic() - t0, 1e-9)
                logger.info(
                    f"  {i:,}/{len(todo):,} units  ({rate:.1f}/s, ~{(len(todo) - i) / max(rate, 1e-9) / 60:.0f} min left)"
                )
    return details


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument(
        "--force-detail",
        action="store_true",
        help="run the detail pull even if the release is unchanged",
    )
    ap.add_argument(
        "--no-detail",
        action="store_true",
        help="never run the detail pull; carry previous details over",
    )
    ap.add_argument(
        "--max-detail", type=int, default=None, help="cap detail calls (smoke tests)"
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="write parquet to data/cache/gem/ instead of Neon",
    )
    ap.add_argument(
        "--min-interval",
        type=float,
        default=0.25,
        help="seconds between API calls (default 0.25)",
    )
    args = ap.parse_args()

    client = GemClient(min_interval=args.min_interval)
    engine = None if args.dry_run else get_engine()

    # 1. release + core pull --------------------------------------------------
    releases = releases_from_sources(client.sources())
    logger.info("GEM releases: " + ", ".join(f"{k}={v}" for k, v in releases.items()))
    fetched_at = now_utc()
    items: dict[str, list[dict]] = {}
    api_totals: dict[str, int] = {}
    for (
        slug,
        tracker,
    ) in TRACKERS.items():  # coal first: its names win at multi-tracker sites
        api_totals[tracker] = client.asset_total(slug)
        items[tracker] = list(client.list_assets(slug))
        logger.info(
            f"{tracker}: {len(items[tracker]):,} units listed (API total {api_totals[tracker]:,})"
        )
    locations, units = build_core_frames(items, releases, fetched_at)

    # 2. detail pull (once per release) ----------------------------------------
    prev_units, prev_locs, prev_release = (
        (pd.DataFrame(), pd.DataFrame(), None)
        if args.dry_run
        else read_previous(engine)
    )
    release = releases["GCPT"]
    need_detail = args.force_detail or (not args.no_detail and release != prev_release)
    snapshots = pd.DataFrame()
    if need_detail:
        reason = (
            "forced"
            if args.force_detail
            else f"release changed ({prev_release!r} → {release!r})"
        )
        logger.info(f"detail pull: {reason}")
        for attempt in (1, 2):
            scope = detail_scope(units)
            details = run_detail_pull(
                client,
                scope,
                CACHE_DIR / f"details_{_slug(release)}.jsonl",
                args.max_detail,
            )
            after = releases_from_sources(client.sources())
            if after["GCPT"] == release:
                break
            logger.warning(
                f"GEM released {after['GCPT']!r} during the pull — discarding and re-pulling core"
            )
            releases, release = after, after["GCPT"]
            items["GCPT"] = list(client.list_assets("coal-plant"))
            locations, units = build_core_frames(items, releases, fetched_at)
            if attempt == 2:
                raise GemApiError("release changed twice during the pull; giving up")
        units, locations, snapshots = apply_details(units, locations, details, release)
    else:
        logger.info(
            f"detail pull skipped (release {release!r} unchanged); carrying previous details over"
        )
        units, locations = carry_over_details(units, locations, prev_units, prev_locs)

    # 3. post-conditions --------------------------------------------------------
    problems = check_frames(locations, units, api_totals)
    if problems:
        for p in problems:
            logger.error(f"post-condition failed: {p}")
        return 2
    logger.success(
        f"{len(locations):,} locations · {len(units):,} units "
        f"({', '.join(f'{t}={int((units.tracker == t).sum()):,}' for t in TRACKERS.values())}) · "
        f"{len(snapshots):,} status snapshots · {client.calls:,} API calls"
    )

    # 4. write ------------------------------------------------------------------
    locations = locations.copy()
    locations["trackers"] = locations["trackers"].apply(
        lambda t: "{" + ",".join(t) + "}"
    )  # text[] literal
    if args.dry_run:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        for name, df in (
            ("gem_locations", locations),
            ("gem_units", units),
            ("gem_unit_status_snapshots", snapshots),
        ):
            df.to_parquet(CACHE_DIR / f"{name}.parquet", index=False)
        logger.info(f"dry run: parquet written to {CACHE_DIR}")
        return 0
    _atomic_replace_table(
        engine, locations, "gem_locations", POST_LOAD_SQL["gem_locations"]
    )
    with engine.begin() as conn:  # text[] column type after to_sql's TEXT
        conn.execute(
            text(
                "ALTER TABLE gem_locations ALTER COLUMN trackers TYPE TEXT[] USING trackers::TEXT[]"
            )
        )
    _atomic_replace_table(engine, units, "gem_units", POST_LOAD_SQL["gem_units"])
    if not snapshots.empty:
        _atomic_replace_table(
            engine,
            snapshots,
            "gem_unit_status_snapshots",
            POST_LOAD_SQL["gem_unit_status_snapshots"],
        )
    else:
        logger.info(
            "no status snapshots this run; gem_unit_status_snapshots left as is"
        )
    with engine.begin() as conn:
        conn.execute(text(EXTERNAL_IDS_DDL))
    logger.success("gem_* tables written")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except GemApiError as e:
        logger.error(str(e))
        sys.exit(1)

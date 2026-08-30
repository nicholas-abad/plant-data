"""GEM reference lookups for the crosswalk build, read from the gem_* tables.

The tables are written by scripts/fetch_gem.py from GEM's Ownership API — the
sole reference source (2026-08-30). This module turns them into the shapes the
matching stages already consume, so build_crosswalk.py keeps its pipeline and
only its reference *source* changes:

* ``name_index(...)``  → ``{display name: info}`` exactly like the old CSV-based
  ``load_gem`` returned, with two additions: every info carries its
  ``gem_location_id``, and a location's alternative / local-language names are
  extra keys pointing at the same info, so the fuzzy matcher sees aliases.
* ``unit(...)``        → one GCPT unit (for the NPP-GIPT unit-level file).
* ``location(...)``    → derived attributes for a linked location: coordinates,
  coal type, combustion technology, site capacity (operating coal units only —
  cancelled and retired units must never count towards nameplate).
* ``resolve_name(...)``→ exact normalized name → location within a country
  (grandfathering today's name-based matches into GEM IDs).

Loaded once per process from Neon; falls back to the parquet written by a
``fetch_gem.py --dry-run`` so the build can be exercised without a database.
"""

from __future__ import annotations

from collections import Counter
from functools import lru_cache
from pathlib import Path

import pandas as pd
from loguru import logger

from .plant_name_matchers.normalizers import normalize_for_comparison

CACHE_DIR = Path(__file__).resolve().parents[1] / "data" / "cache" / "gem"

_TABLES: dict[str, pd.DataFrame] | None = None


def _coal_type(v) -> str | None:
    """API coal_type → dashboard vocabulary (bituminous, subbituminous, lignite, anthracite, waste; None for unknown)."""
    if not isinstance(v, str):
        return None
    s = v.strip().lower()
    if not s or s == "unknown":
        return None
    s = s.replace(" with ccs", "")
    if s == "waste coal":
        return "waste"
    return s


def _tech(v) -> str | None:
    from .build_crosswalk import (
        _normalize_combustion_tech,
    )  # local import: avoid a cycle

    if isinstance(v, str) and "/" in v:  # 'subcritical/CCS'
        v = v.split("/", 1)[0]
    return _normalize_combustion_tech(v)


def load_tables(engine=None, force: bool = False) -> dict[str, pd.DataFrame]:
    """gem_locations / gem_units as DataFrames, cached for the process."""
    global _TABLES
    if _TABLES is not None and not force:
        return _TABLES
    if engine is not None:
        with engine.connect() as conn:
            locs = pd.read_sql("SELECT * FROM gem_locations", conn)
            units = pd.read_sql("SELECT * FROM gem_units", conn)
        logger.info(
            f"GEM reference from Neon: {len(locs):,} locations, {len(units):,} units"
        )
    else:
        locs = pd.read_parquet(CACHE_DIR / "gem_locations.parquet")
        units = pd.read_parquet(CACHE_DIR / "gem_units.parquet")
        logger.warning(f"GEM reference from parquet cache ({CACHE_DIR}) — not Neon")
    if locs.empty or units.empty:
        raise RuntimeError(
            "gem_locations / gem_units are empty — run scripts/fetch_gem.py first"
        )
    units["is_coal"] = units["tracker"] == "GCPT"
    units["is_operating"] = units["status"].astype(str).str.lower() == "operating"
    _TABLES = {
        "locations": locs.set_index("gem_location_id", drop=False),
        "units": units,
    }
    _site_attrs.cache_clear()
    return _TABLES


@lru_cache(maxsize=None)
def _site_attrs(loc_id: str) -> dict:
    """Coal attributes of one location from its GCPT units."""
    t = load_tables()
    u = t["units"]
    coal = u[(u["gem_location_id"] == loc_id) & u["is_coal"]]
    if coal.empty:
        return {
            "_is_coal": False,
            "coal_type": None,
            "combustion_tech": None,
            "capacity_mw": None,
        }
    op = coal[coal["is_operating"]]
    cap = (
        float(op["capacity_mw"].dropna().sum())
        if op["capacity_mw"].notna().any()
        else None
    )
    # Capacity-weighted mode of coal type / technology across the coal units
    # (operating first; fall back to all coal units for a fully retired site).
    basis = op if not op.empty else coal
    ct = Counter()
    tech = Counter()
    for r in basis.itertuples():
        w = float(r.capacity_mw) if pd.notna(r.capacity_mw) else 1.0
        c = _coal_type(r.coal_type)
        if c:
            ct[c] += w
        tt = _tech(r.combustion_tech)
        if tt:
            tech[tt] += w
    return {
        "_is_coal": True,
        "coal_type": ct.most_common(1)[0][0] if ct else None,
        "combustion_tech": tech.most_common(1)[0][0] if tech else None,
        "capacity_mw": cap if cap and cap > 0 else None,
    }


def location(loc_id: str) -> dict | None:
    """Everything a crosswalk row derives from its linked GEM location."""
    t = load_tables()
    if loc_id not in t["locations"].index:
        return None
    L = t["locations"].loc[loc_id]
    attrs = _site_attrs(loc_id)
    return {
        "gem_location_id": loc_id,
        "name": L["name"],
        "country": L["country"],
        "lat": L["latitude"],
        "lon": L["longitude"],
        **attrs,
    }


def name_index(
    country: str | None = None, countries: list[str] | None = None
) -> dict[str, dict]:
    """{name or alias: info} for the given country / countries (all GEM if neither).

    Mirrors the old CSV ``load_gem`` output so the fuzzy stage is unchanged.
    """
    t = load_tables()
    locs = t["locations"]
    if country:
        locs = locs[locs["country"] == country]
    elif countries:
        locs = locs[locs["country"].isin(countries)]
    out: dict[str, dict] = {}
    for L in locs.itertuples():
        info = location(L.gem_location_id)
        if info is None or pd.isna(info["lat"]) or pd.isna(info["lon"]):
            continue
        for key in (
            L.name,
            getattr(L, "name_other", None),
            getattr(L, "name_local", None),
        ):
            if isinstance(key, str) and key.strip() and key not in out:
                out[key] = info
            elif (
                isinstance(key, str)
                and key in out
                and info["_is_coal"]
                and not out[key]["_is_coal"]
            ):
                out[key] = info  # coal site beats a same-named non-coal one
    return out


def unit(gem_unit_id: str) -> dict | None:
    """One GCPT unit: capacity, coal type, tech, its location and coordinates."""
    t = load_tables()
    u = t["units"]
    hit = u[u["gem_unit_id"] == gem_unit_id]
    if hit.empty:
        return None
    r = hit.iloc[0]
    L = (
        t["locations"].loc[r["gem_location_id"]]
        if r["gem_location_id"] in t["locations"].index
        else None
    )
    return {
        "gem_unit_id": gem_unit_id,
        "gem_location_id": r["gem_location_id"],
        "capacity_mw": float(r["capacity_mw"]) if pd.notna(r["capacity_mw"]) else None,
        "coal_type": _coal_type(r["coal_type"]) if r["tracker"] == "GCPT" else None,
        "combustion_tech": _tech(r["combustion_tech"])
        if r["tracker"] == "GCPT"
        else None,
        "status": r["status"],
        "lat": None if L is None else L["latitude"],
        "lon": None if L is None else L["longitude"],
        "name": None if L is None else L["name"],
    }


def resolve_name(name: str, country: str | None) -> str | None:
    """Exact normalized-name → gem_location_id within a country; None if absent or ambiguous."""
    if not isinstance(name, str) or not name.strip():
        return None
    t = load_tables()
    locs = t["locations"]
    if country:
        locs = locs[locs["country"] == country]
    key = normalize_for_comparison(name)
    if not key:
        return None
    hits = set()
    for L in locs.itertuples():
        for cand in (
            L.name,
            getattr(L, "name_other", None),
            getattr(L, "name_local", None),
        ):
            if isinstance(cand, str) and normalize_for_comparison(cand) == key:
                hits.add(L.gem_location_id)
                break
    return hits.pop() if len(hits) == 1 else None


def country_of(loc_id: str) -> str | None:
    t = load_tables()
    return (
        t["locations"].at[loc_id, "country"] if loc_id in t["locations"].index else None
    )

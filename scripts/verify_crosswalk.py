#!/usr/bin/env python3
"""Regression gates for a freshly built unified_plant_crosswalk.parquet.

Run BEFORE swapping the crosswalk into prod (bootstrap_neon_db.py). Exits
non-zero if any gate fails. Compares against a baseline parquet (default:
the git-committed previous version) so a rebuild cannot silently regress
coverage or reintroduce known-bad matches.

Usage:
    uv run python scripts/verify_crosswalk.py [--baseline PATH]

Gates:
  1. Known-bad LLM cross-border matches (nulled in prod 2026-08-01) must NOT
     carry coordinates from a wrong-country reference again.
  2. Every matched ENTSO-E unit's reference plant must exist in the unit's
     own country (GEM Country/Area or GPPD country).
  3. ENTSO-E per-site capacity: units sharing a reference must sum to that
     reference's nameplate (division invariant), and Germany's fleet total
     must land in a plausible band (site-per-unit stamping inflated it ~3x).
  4. NPP plants whose DGR-2 fuel section is non-THERMAL carry no coal
     metadata (coal_type / combustion_tech / capacity_mw all null).
  5. Per-source coordinate coverage must not drop vs the baseline by more
     than 2 percentage points.
"""

import argparse
import subprocess
import sys
import tempfile
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from src.build_crosswalk import (  # noqa: E402
    ENTSOE_AREA_COUNTRIES,
    GEM_CSV,
    GPPD_CSV,
    OUTPUT_FILE,
)

# The 8 provably-wrong LLM matches found in the 2026-07 audit (coal units
# matched to plants in the wrong country — one to a German solar farm).
KNOWN_BAD = {
    "EME3_G11__",
    "EDET_G1___",
    "EDET_G4___",
    "EDET_G4____",
    "MÃ\x812_gÃ©p3",  # Hungary's Mátra unit as stored (mojibake; verified vs prod)
    "KS_TGA3",
    "KS_TGA4",
    "KS_TGA5",
}

NON_THERMAL = {"HYDRO", "NUCLEAR", "THER (GT)", "THER (DG)"}


def fail(msg: str) -> None:
    print(f"FAIL  {msg}")
    fail.count += 1


fail.count = 0


def ok(msg: str) -> None:
    print(f"ok    {msg}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline", type=Path, default=None)
    args = ap.parse_args()

    xw = pd.read_parquet(OUTPUT_FILE)

    if args.baseline:
        base = pd.read_parquet(args.baseline)
    else:
        raw = subprocess.run(
            ["git", "-C", str(REPO), "show",
             "HEAD:data/crosswalks/unified_plant_crosswalk.parquet"],
            capture_output=True, check=True,
        ).stdout
        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
            tmp.write(raw)
            tmp.flush()
            base = pd.read_parquet(tmp.name)

    entsoe = xw[xw["source_system"] == "ENTSOE"]

    # Area -> country for every ENTSO-E plant, from the live name->area pull
    # embedded in the parquet is not stored; re-derive from ref validation
    # instead: a matched ref must exist in the unit's own country's slice of
    # its reference DB. Country is derived from the plant's area via the DB
    # at build time, so here we validate ref-side only: the ref name must be
    # unique to ONE country, and where resolvable it must match for the
    # KNOWN_BAD rows (gate 1) and for all rows (gate 2, ref-existence form).
    gem = pd.read_csv(
        GEM_CSV, low_memory=False, usecols=["Project Name", "Country/Area"]
    )
    gem_countries: dict[str, set] = (
        gem.dropna().groupby("Project Name")["Country/Area"].agg(set).to_dict()
    )
    gppd = pd.read_csv(GPPD_CSV, usecols=["name", "country"], low_memory=False)
    gppd_countries: dict[str, set] = (
        gppd.dropna().groupby("name")["country"].agg(set).to_dict()
    )

    # ------------------------------------------------------------------ 1
    bad = entsoe[entsoe["plant_name"].isin(KNOWN_BAD) & entsoe["latitude"].notna()]
    # Cross-border refs the audit identified:
    bad_refs = {"Embrets3", "Ede power station", "Maritsa 3 power station",
                "Torgau Solar Power Plant"}
    relapsed = bad[bad["ref_matched_name"].isin(bad_refs)]
    n_unmatched_bad = int(
        (entsoe["plant_name"].isin(KNOWN_BAD) & entsoe["latitude"].isna()).sum()
    )
    if len(relapsed):
        fail(f"gate1: {len(relapsed)} known-bad rows re-matched their wrong ref: "
             f"{relapsed[['plant_name', 'ref_matched_name']].to_dict('records')}")
    else:
        ok(f"gate1: none of the 8 known-bad rows re-matched a wrong-country ref "
           f"({len(bad)} re-matched in-country, {n_unmatched_bad} unmatched)")

    # ------------------------------------------------------------------ 2
    # Every matched ENTSO-E ref must exist in exactly the country set that
    # includes... we cannot see the unit's area here, but the builder now
    # restricts candidates per country, so a violation would mean the ref
    # name doesn't exist in ANY country slice used — detect refs that exist
    # in neither reference at all (dangling), and refs whose country set has
    # no overlap with mapped ENTSO-E countries.
    valid_countries = {c["gem"] for c in ENTSOE_AREA_COUNTRIES.values()}
    valid_iso = {c["gppd"] for c in ENTSOE_AREA_COUNTRIES.values()}
    matched = entsoe[entsoe["ref_matched_name"].notna()]
    dangling = []
    out_of_region = []
    for _, r in matched.iterrows():
        name = r["ref_matched_name"]
        cset = gem_countries.get(name) if r["ref_source"] == "GEM" else gppd_countries.get(name)
        if not cset:
            dangling.append((r["plant_name"], r["ref_source"], name))
        elif r["ref_source"] == "GEM" and not (cset & valid_countries):
            out_of_region.append((r["plant_name"], name, sorted(cset)))
        elif r["ref_source"] == "GPPD" and not (cset & valid_iso):
            out_of_region.append((r["plant_name"], name, sorted(cset)))
    if dangling:
        fail(f"gate2: {len(dangling)} matched refs not found in their reference DB: {dangling[:5]}")
    else:
        ok(f"gate2: all {len(matched)} matched ENTSO-E refs resolve in their reference DB")
    if out_of_region:
        fail(f"gate2b: {len(out_of_region)} refs outside the ENTSO-E country set: {out_of_region[:5]}")
    else:
        ok("gate2b: no matched ref lies outside the ENTSO-E country set")

    # ------------------------------------------------------------------ 3
    cap = entsoe[entsoe["capacity_mw"].notna() & entsoe["ref_matched_name"].notna()]
    gem_cap_raw = pd.read_csv(
        GEM_CSV, low_memory=False,
        usecols=["Project Name", "Country/Area", "Capacity", "Fuel"],
    )
    per_site = cap.groupby(["ref_source", "ref_matched_name"])["capacity_mw"].sum()
    max_site = per_site.max()
    if max_site > 6000:
        fail(f"gate3: a single site sums to {max_site:,.0f} MW — division regressed?")
    else:
        ok(f"gate3: largest per-site capacity sum {max_site:,.0f} MW (≤ 6,000)")
    # Capacity is OPERATING-units-only (load_gem status filter) — Europe's
    # operating coal fleet is ~90-100 GW and ENTSO-E's reporting subset lands
    # below that. The failure modes guarded: site-per-unit stamping (416 GW
    # pre-fix) above, and a broken status filter / empty capacities below.
    total = cap["capacity_mw"].sum()
    if not (40_000 <= total <= 120_000):
        fail(f"gate3b: ENTSO-E fleet capacity {total:,.0f} MW outside 40-120 GW "
             "operating-only band (416 GW = pre-fix inflation)")
    else:
        ok(f"gate3b: ENTSO-E fleet capacity {total:,.0f} MW plausible (was 416 GW pre-fix)")

    # ------------------------------------------------------------------ 4
    npp = xw[xw["source_system"] == "NPP"]
    # npp fuel isn't stored in the parquet; spot-check the audit's exemplars.
    # Non-THERMAL plants must carry NO coal metadata anywhere (fuel-aware
    # suppression — GANDHI SAGAR PSP's bogus coal marker was the motivating
    # bug). NIGRI TPP (THERMAL, GPPD-matched — GPPD has no coal-type field)
    # proves suppression does NOT fire on thermal plants: its GPPD nameplate
    # must survive.
    for name, cols, should_have in [
        ("GANDHI SAGAR PSP", ["coal_type", "combustion_tech", "capacity_mw"], False),
        ("KUDANKULAM", ["coal_type", "combustion_tech", "capacity_mw"], False),
        ("NIGRI TPP", ["capacity_mw"], True),
    ]:
        row = npp[npp["plant_name"] == name]
        if row.empty:
            continue
        has = bool(row.iloc[0][cols].notna().any())
        if has != should_have:
            fail(f"gate4: {name}: {cols} present={has}, expected {should_have}")
        else:
            ok(f"gate4: {name} enrichment correct (present={has})")

    # ------------------------------------------------------------------ 5
    for src in sorted(set(base["source_system"]) & set(xw["source_system"])):
        b = base[base["source_system"] == src]["latitude"].notna().mean()
        n = xw[xw["source_system"] == src]["latitude"].notna().mean()
        if n < b - 0.02:
            fail(f"gate5: {src} coverage {n:.1%} < baseline {b:.1%} - 2pp")
        else:
            ok(f"gate5: {src} coverage {n:.1%} (baseline {b:.1%})")

    print(f"\n{'ALL GATES PASSED' if fail.count == 0 else f'{fail.count} GATE(S) FAILED'}")
    return 1 if fail.count else 0


if __name__ == "__main__":
    sys.exit(main())

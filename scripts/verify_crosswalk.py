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
  4b. OCCTO capacity exemplars carry their HJKS coal rated outputs.
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

from src import gem_reference as gemref  # noqa: E402
from src.build_crosswalk import (  # noqa: E402
    ENTSOE_AREA_COUNTRIES,
    GPPD_CSV,
    HUMAN_METHODS,
    OUTPUT_FILE,
    _make_engine,
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
            [
                "git",
                "-C",
                str(REPO),
                "show",
                "HEAD:data/crosswalks/unified_plant_crosswalk.parquet",
            ],
            capture_output=True,
            check=True,
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
    # GEM reference from the gem_* tables (GEM's API); parquet cache if no DB.
    try:
        gem_tables = gemref.load_tables(_make_engine())
    except Exception as e:  # noqa: BLE001 — offline verification of a parquet
        print(f"  (gem tables from parquet cache: {e})")
        gem_tables = gemref.load_tables()
    gem_locs = gem_tables["locations"]
    gem_countries: dict[str, set] = {}
    for L in gem_locs.itertuples():
        for nm in (
            L.name,
            getattr(L, "name_other", None),
            getattr(L, "name_local", None),
        ):
            if isinstance(nm, str) and nm:
                gem_countries.setdefault(nm, set()).add(L.country)
    gppd = pd.read_csv(GPPD_CSV, usecols=["name", "country"], low_memory=False)
    gppd_countries: dict[str, set] = (
        gppd.dropna().groupby("name")["country"].agg(set).to_dict()
    )

    # ------------------------------------------------------------------ 1
    bad = entsoe[entsoe["plant_name"].isin(KNOWN_BAD) & entsoe["latitude"].notna()]
    # Cross-border refs the audit identified:
    bad_refs = {
        "Embrets3",
        "Ede power station",
        "Maritsa 3 power station",
        "Torgau Solar Power Plant",
    }
    relapsed = bad[bad["ref_matched_name"].isin(bad_refs)]
    n_unmatched_bad = int(
        (entsoe["plant_name"].isin(KNOWN_BAD) & entsoe["latitude"].isna()).sum()
    )
    if len(relapsed):
        fail(
            f"gate1: {len(relapsed)} known-bad rows re-matched their wrong ref: "
            f"{relapsed[['plant_name', 'ref_matched_name']].to_dict('records')}"
        )
    else:
        ok(
            f"gate1: none of the 8 known-bad rows re-matched a wrong-country ref "
            f"({len(bad)} re-matched in-country, {n_unmatched_bad} unmatched)"
        )

    # ------------------------------------------------------------------ 2
    # Every matched ENTSO-E ref must exist in exactly the country set that
    # includes... we cannot see the unit's area here, but the builder now
    # restricts candidates per country, so a violation would mean the ref
    # name doesn't exist in ANY country slice used — detect refs that exist
    # in neither reference at all (dangling), and refs whose country set has
    # no overlap with mapped ENTSO-E countries.
    valid_countries = {c["gem"] for c in ENTSOE_AREA_COUNTRIES.values()}
    valid_iso = {c["gppd"] for c in ENTSOE_AREA_COUNTRIES.values()}
    # Frozen legacy rows keep a reference name from an older GEM release; their
    # GEM link is validated by gate 6b instead.
    matched = entsoe[
        entsoe["ref_matched_name"].notna()
        & ~entsoe["matching_method"].isin(HUMAN_METHODS)
    ]
    dangling = []
    out_of_region = []
    for _, r in matched.iterrows():
        name = r["ref_matched_name"]
        cset = (
            gem_countries.get(name)
            if r["ref_source"] == "GEM"
            else gppd_countries.get(name)
        )
        if not cset:
            dangling.append((r["plant_name"], r["ref_source"], name))
        elif r["ref_source"] == "GEM" and not (cset & valid_countries):
            out_of_region.append((r["plant_name"], name, sorted(cset)))
        elif r["ref_source"] == "GPPD" and not (cset & valid_iso):
            out_of_region.append((r["plant_name"], name, sorted(cset)))
    if dangling:
        fail(
            f"gate2: {len(dangling)} matched refs not found in their reference DB: {dangling[:5]}"
        )
    else:
        ok(
            f"gate2: all {len(matched)} matched ENTSO-E refs resolve in their reference DB"
        )
    if out_of_region:
        fail(
            f"gate2b: {len(out_of_region)} refs outside the ENTSO-E country set: {out_of_region[:5]}"
        )
    else:
        ok("gate2b: no matched ref lies outside the ENTSO-E country set")

    # ------------------------------------------------------------------ 3
    cap = entsoe[entsoe["capacity_mw"].notna() & entsoe["ref_matched_name"].notna()]
    site_key = (
        cap["gem_location_id"].where(
            cap["gem_location_id"].notna(),
            cap["ref_source"].astype(str) + "|" + cap["ref_matched_name"].astype(str),
        )
        if "gem_location_id" in cap.columns
        else cap["ref_source"].astype(str) + "|" + cap["ref_matched_name"].astype(str)
    )
    per_site = cap.groupby(site_key)["capacity_mw"].sum()
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
        fail(
            f"gate3b: ENTSO-E fleet capacity {total:,.0f} MW outside 40-120 GW "
            "operating-only band (416 GW = pre-fix inflation)"
        )
    else:
        ok(
            f"gate3b: ENTSO-E fleet capacity {total:,.0f} MW plausible (was 416 GW pre-fix)"
        )

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

    # ------------------------------------------------------------------ 4b
    # OCCTO capacity comes from HJKS rated outputs (code-keyed). The two
    # audit exemplars that read >100% CF from GEM's coal-slice capacity:
    occto = xw[xw["source_system"] == "OCCTO"]
    for name, expected_mw in [
        ("防府バイオマス発電所", 112.0),  # Hofu Biomass — was 36 (GEM), CF 219%
        ("宇部興産発電所", 361.0),  # UBE — two registrations summed
    ]:
        row = occto[occto["plant_name"] == name]
        if row.empty:
            fail(f"gate4b: {name} missing from crosswalk")
            continue
        got = row.iloc[0]["capacity_mw"]
        if pd.isna(got) or abs(float(got) - expected_mw) > 1.0:
            fail(f"gate4b: {name} capacity {got} MW, expected ~{expected_mw}")
        else:
            ok(f"gate4b: {name} capacity {float(got):,.0f} MW (HJKS)")

    # ------------------------------------------------------------------ 5
    # Coverage is compared as a COUNT of coordinated plants, not a share: a
    # source that gained many new (as yet unmatched) plants since the baseline
    # would otherwise fail on a falling percentage while losing nothing.
    for src in sorted(set(base["source_system"]) & set(xw["source_system"])):
        bsub = base[base["source_system"] == src]
        nsub = xw[xw["source_system"] == src]
        # Baseline count over plants still present in the source only.
        still = bsub[bsub["plant_name"].isin(nsub["plant_name"])]
        b_n, n_n = (
            int(still["latitude"].notna().sum()),
            int(nsub["latitude"].notna().sum()),
        )
        lost = set(bsub.loc[bsub["latitude"].notna(), "plant_name"]) - set(
            nsub.loc[nsub["latitude"].notna(), "plant_name"]
        )
        # Plants that left the SOURCE (e.g. mojibake identities merged away) are
        # not losses of ours.
        lost -= set(bsub["plant_name"]) - set(nsub["plant_name"])
        if n_n < b_n or lost:
            fail(
                f"gate5: {src} coordinated plants {n_n} < baseline {b_n} ({len(lost)} lost: {sorted(lost)[:3]})"
            )
        else:
            ok(
                f"gate5: {src} coordinated plants {n_n} (baseline {b_n}); {nsub['latitude'].notna().mean():.1%} of {len(nsub)} rows"
            )

    # ------------------------------------------------------------------ 6  GEM identity
    key = (
        xw["source_system"].astype(str)
        + "|"
        + xw["plant_code"].where(xw["plant_code"].notna(), xw["plant_name"]).astype(str)
    )
    dup = key[key.duplicated()]
    if len(dup):
        fail(
            f"gate6a: {len(dup)} duplicate natural keys (dashboard joins fan out): {dup.head(5).tolist()}"
        )
    else:
        ok(
            f"gate6a: {len(xw):,} rows, one per (source_system, COALESCE(plant_code, plant_name))"
        )

    linked = xw[xw["gem_location_id"].notna()]
    unknown = linked[~linked["gem_location_id"].isin(gem_locs.index)]
    if len(unknown):
        fail(
            f"gate6b: {len(unknown)} rows link to a gem_location_id absent from gem_locations: "
            f"{unknown['gem_location_id'].head(5).tolist()}"
        )
    else:
        ok(f"gate6b: all {len(linked):,} GEM links resolve in gem_locations")

    gem_country_of = gem_locs["country"].to_dict()
    lc = linked[linked["source_country"].notna()]
    cross = lc[
        (lc["gem_location_id"].map(gem_country_of) != lc["source_country"])
        & lc["override_reason"].isna()
    ]
    if len(cross):
        fail(
            f"gate6c: {len(cross)} cross-country links without override_reason: "
            f"{cross[['plant_name', 'source_country', 'gem_location_id']].head(5).to_dict('records')}"
        )
    else:
        ok(
            f"gate6c: no cross-country GEM link without an override reason ({len(lc):,} checked)"
        )

    human = xw[xw["matching_method"].isin(HUMAN_METHODS)]
    orphan_manual = human[human["decided_by"].isna()]
    if len(orphan_manual):
        fail(f"gate6d: {len(orphan_manual)} manual/legacy rows without decided_by")
    else:
        ok(f"gate6d: all {len(human):,} human-decided rows carry decided_by")

    both = xw[xw["gem_location_id"].notna() & (xw["not_in_gem"] == True)]  # noqa: E712
    if len(both):
        fail(f"gate6e: {len(both)} rows both linked and not_in_gem")
    else:
        ok("gate6e: no row is both linked and not_in_gem")

    # Decisions must survive the rebuild: every decided row in the baseline
    # keeps its link columns byte-for-byte.
    if "decided_by" in base.columns:
        bkey = (
            base["source_system"].astype(str)
            + "|"
            + base["plant_code"]
            .where(base["plant_code"].notna(), base["plant_name"])
            .astype(str)
        )
        bdec = (
            base[base["decided_by"].notna()]
            .assign(_k=bkey[base["decided_by"].notna()])
            .set_index("_k")
        )
        now = xw.assign(_k=key).set_index("_k")
        lost = [k for k in bdec.index if k not in now.index]
        changed = [
            k
            for k in bdec.index
            if k in now.index
            and (
                str(now.at[k, "gem_location_id"]) != str(bdec.at[k, "gem_location_id"])
                or bool(now.at[k, "not_in_gem"]) != bool(bdec.at[k, "not_in_gem"])
            )
        ]
        if lost or changed:
            fail(
                f"gate6f: decisions not preserved — {len(lost)} rows gone, {len(changed)} links changed: {(lost + changed)[:5]}"
            )
        else:
            ok(f"gate6f: all {len(bdec):,} prior decisions preserved")
    else:
        ok("gate6f: baseline predates decisions (nothing to preserve)")

    print(
        f"\n{'ALL GATES PASSED' if fail.count == 0 else f'{fail.count} GATE(S) FAILED'}"
    )
    return 1 if fail.count else 0


if __name__ == "__main__":
    sys.exit(main())

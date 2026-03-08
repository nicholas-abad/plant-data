#!/usr/bin/env python3
"""Build unified plant coordinate crosswalk from all 5 generation sources.

Produces a single parquet file mapping every unique plant name (from EIA,
ENTSOE, NPP, ONS, OE) to coordinates, with an audit trail of how each
was matched.

Pipeline:
  1. Pull distinct plant names from each generation table in Neon
  2. Load reference databases (GEM CSV, GPPD from Neon)
  3. Direct matching (OE embedded coords)
  4. Rapidfuzz matching against GEM + GPPD (unmatched only)
  5. LLM matching via GeminiNameMatcher (unmatched only)
  6. Save unified_plant_crosswalk.parquet

Usage:
    cd data/plant-data
    python -m src.build_crosswalk          # run full pipeline
    python -m src.build_crosswalk --no-llm # skip LLM step
"""

import os
import sys

import pandas as pd
from dotenv import load_dotenv
from loguru import logger
from rapidfuzz import fuzz, process
from sqlalchemy import create_engine, text

from .plant_name_matchers import (
    CandidateRetriever,
    GeminiNameMatcher,
    normalize_for_comparison,
    normalize_gppd_name,
)
from .utils import get_data_dir, get_crosswalk_dir, validate_coordinates

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
OUTPUT_DIR = get_crosswalk_dir()
OUTPUT_FILE = OUTPUT_DIR / "unified_plant_crosswalk.parquet"

GEM_CSV = get_data_dir() / "GEM database_21Feb2026.csv"
GPPD_CSV = get_crosswalk_dir() / "global_power_plant_database.csv"

# Rapidfuzz thresholds (same as notebook / dashboard)
GEM_THRESHOLD = 80
GPPD_THRESHOLD = 80

# Country filters for each source when querying GPPD / GEM
SOURCE_COUNTRIES = {
    "NPP": {"gppd": "IND", "gem": "India"},
    "ENTSOE": {"gppd_countries": [
        "AUT", "BEL", "BGR", "HRV", "CZE", "DNK", "EST", "FIN", "FRA",
        "DEU", "GRC", "HUN", "IRL", "ITA", "LVA", "LTU", "LUX", "NLD",
        "POL", "PRT", "ROU", "SVK", "SVN", "ESP", "SWE", "GBR", "NOR",
        "CHE", "SRB", "BIH", "MNE", "MKD", "ALB", "XKX",
    ]},
    "EIA": {"gppd": "USA", "gem": "United States of America"},
    "ONS": {"gppd": "BRA", "gem": "Brazil"},
    "OE": {"gppd": "AUS", "gem": "Australia"},
}

# Columns in the output
OUTPUT_COLUMNS = [
    "plant_name", "source_system", "latitude", "longitude",
    "ref_source", "matching_method", "confidence", "ref_matched_name",
]


def _make_engine():
    """Create SQLAlchemy engine from environment variables."""
    load_dotenv()
    url = os.environ.get("DATABASE_URL")
    if url:
        return create_engine(url, connect_args={"connect_timeout": 30})
    # Fall back to individual env vars
    host = os.environ["POSTGRES_HOST"]
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ["POSTGRES_DB"]
    user = os.environ["POSTGRES_USER"]
    pw = os.environ["POSTGRES_PASSWORD"]
    ssl = os.environ.get("POSTGRES_SSLMODE", "require")
    return create_engine(
        f"postgresql://{user}:{pw}@{host}:{port}/{db}?sslmode={ssl}",
        connect_args={"connect_timeout": 30},
    )


# ---------------------------------------------------------------------------
# Step 1: Pull distinct plant names
# ---------------------------------------------------------------------------
def pull_plant_names(engine) -> pd.DataFrame:
    """Pull distinct plant names from all 5 generation tables."""
    queries = {
        "NPP": "SELECT DISTINCT plant AS plant_name FROM npp_generation WHERE plant IS NOT NULL",
        "ENTSOE": "SELECT DISTINCT plant_name FROM entsoe_generation WHERE plant_name IS NOT NULL",
        "EIA": "SELECT DISTINCT plant_code, plant_name FROM eia_generation_data WHERE plant_name IS NOT NULL",
        "ONS": "SELECT DISTINCT plant AS plant_name FROM ons_generation WHERE plant IS NOT NULL",
        "OE": "SELECT DISTINCT facility_name AS plant_name, latitude, longitude FROM oe_facility_generation_data WHERE facility_name IS NOT NULL",
    }

    frames = []
    with engine.connect() as conn:
        conn.execute(text("SET statement_timeout = '120s'"))
        for source, sql in queries.items():
            logger.info(f"Pulling {source} plant names...")
            df = pd.read_sql(text(sql), conn)
            df["source_system"] = source
            frames.append(df)
            logger.info(f"  {source}: {len(df):,} distinct plants")

    return pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------------------------
# Step 2: Load reference databases
# ---------------------------------------------------------------------------
def load_gem(source_system: str | None = None) -> dict[str, dict]:
    """Load GEM CSV filtered by country. Returns {name: {lat, lon, name}}."""
    if not GEM_CSV.exists():
        logger.warning(f"GEM CSV not found: {GEM_CSV}")
        return {}

    gem_raw = pd.read_csv(GEM_CSV, low_memory=False)
    if source_system and source_system in SOURCE_COUNTRIES:
        cfg = SOURCE_COUNTRIES[source_system]
        gem_country = cfg.get("gem")
        if gem_country:
            gem_raw = gem_raw[gem_raw["Country/Area"] == gem_country]

    names: dict[str, dict] = {}
    for _, row in gem_raw.iterrows():
        name = row["Project Name"]
        if pd.notna(name) and name not in names:
            names[name] = {"lat": row["Latitude"], "lon": row["Longitude"], "name": name}
    return names


def load_gppd(country_codes: list[str] | None = None) -> pd.DataFrame:
    """Load GPPD entries from local CSV, optionally filtered by country."""
    if not GPPD_CSV.exists():
        logger.warning(f"GPPD CSV not found: {GPPD_CSV}")
        return pd.DataFrame(columns=["name", "latitude", "longitude", "country"])

    gppd = pd.read_csv(GPPD_CSV, usecols=["name", "latitude", "longitude", "country"], low_memory=False)
    if country_codes:
        gppd = gppd[gppd["country"].isin(country_codes)]
    return gppd


# ---------------------------------------------------------------------------
# Step 3: Direct matching (OE embedded coordinates)
# ---------------------------------------------------------------------------
def match_direct(plants_df: pd.DataFrame) -> pd.DataFrame:
    """Direct matching for OE plants that already have embedded coordinates."""
    results = []

    oe_plants = plants_df[plants_df["source_system"] == "OE"].copy()
    if not oe_plants.empty:
        logger.info("OE direct coordinate matching...")
        for _, row in oe_plants.iterrows():
            lat, lon = row.get("latitude"), row.get("longitude")
            if pd.notna(lat) and pd.notna(lon) and validate_coordinates(lat, lon):
                results.append({
                    "plant_name": row["plant_name"],
                    "source_system": "OE",
                    "latitude": lat,
                    "longitude": lon,
                    "ref_source": "OE-direct",
                    "matching_method": "direct",
                    "confidence": None,
                    "ref_matched_name": row["plant_name"],
                })
        logger.info(f"  OE direct: {len(results):,} matched")

    return pd.DataFrame(results, columns=OUTPUT_COLUMNS) if results else pd.DataFrame(columns=OUTPUT_COLUMNS)


# ---------------------------------------------------------------------------
# Step 4: Rapidfuzz matching
# ---------------------------------------------------------------------------
def match_rapidfuzz(
    unmatched: pd.DataFrame,
) -> pd.DataFrame:
    """Rapidfuzz matching against GEM and GPPD."""
    results = []

    for source in unmatched["source_system"].unique():
        src_plants = unmatched[unmatched["source_system"] == source]
        if src_plants.empty:
            continue

        logger.info(f"Rapidfuzz matching {len(src_plants):,} {source} plants...")

        # Load per-source references
        gem_names = load_gem(source)
        gem_norm = {normalize_for_comparison(n): n for n in gem_names}
        gem_norm_list = list(gem_norm.keys())

        # Load GPPD for this source's countries
        cfg = SOURCE_COUNTRIES.get(source, {})
        gppd_countries = cfg.get("gppd_countries") or ([cfg["gppd"]] if cfg.get("gppd") else None)
        gppd_df = load_gppd(gppd_countries)
        gppd_raw_names = gppd_df["name"].dropna().unique().tolist()
        gppd_norm = {normalize_gppd_name(n): n for n in gppd_raw_names}
        gppd_norm_list = list(gppd_norm.keys())
        # Build gppd name -> coords
        gppd_coords: dict[str, dict] = {}
        for _, grow in gppd_df.iterrows():
            n = grow["name"]
            if pd.notna(n) and n not in gppd_coords:
                gppd_coords[n] = {"lat": grow["latitude"], "lon": grow["longitude"]}

        count = 0
        for _, row in src_plants.iterrows():
            plant_name = row["plant_name"]
            if pd.isna(plant_name) or not str(plant_name).strip():
                continue

            norm_name = normalize_for_comparison(plant_name)
            matched = False

            # --- GEM: token_sort_ratio ---
            if gem_norm_list:
                gem_hit = process.extractOne(
                    norm_name, gem_norm_list,
                    scorer=fuzz.token_sort_ratio, score_cutoff=GEM_THRESHOLD,
                )
                if gem_hit:
                    orig = gem_norm[gem_hit[0]]
                    info = gem_names[orig]
                    if pd.notna(info["lat"]) and pd.notna(info["lon"]):
                        results.append({
                            "plant_name": plant_name,
                            "source_system": source,
                            "latitude": info["lat"],
                            "longitude": info["lon"],
                            "ref_source": "GEM",
                            "matching_method": "rapidfuzz",
                            "confidence": None,
                            "ref_matched_name": orig,
                        })
                        matched = True
                        count += 1

            # --- GPPD: token_sort_ratio ---
            if not matched and gppd_norm_list:
                gppd_query = normalize_gppd_name(plant_name)
                gppd_hit = process.extractOne(
                    gppd_query, gppd_norm_list,
                    scorer=fuzz.token_sort_ratio, score_cutoff=GPPD_THRESHOLD,
                )
                if gppd_hit:
                    orig = gppd_norm[gppd_hit[0]]
                    coords = gppd_coords.get(orig, {})
                    if pd.notna(coords.get("lat")) and pd.notna(coords.get("lon")):
                        results.append({
                            "plant_name": plant_name,
                            "source_system": source,
                            "latitude": coords["lat"],
                            "longitude": coords["lon"],
                            "ref_source": "GPPD",
                            "matching_method": "rapidfuzz",
                            "confidence": None,
                            "ref_matched_name": orig,
                        })
                        matched = True
                        count += 1

        logger.info(f"  {source} rapidfuzz: {count:,} matched")

    return pd.DataFrame(results, columns=OUTPUT_COLUMNS) if results else pd.DataFrame(columns=OUTPUT_COLUMNS)


# ---------------------------------------------------------------------------
# Step 5: LLM matching
# ---------------------------------------------------------------------------
def match_llm(
    unmatched: pd.DataFrame,
) -> pd.DataFrame:
    """LLM matching for remaining unmatched plants using GeminiNameMatcher."""
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        logger.warning("GEMINI_API_KEY not set — skipping LLM matching")
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    matcher = GeminiNameMatcher(api_key=api_key)
    logger.info(f"Initialized {matcher.name} matcher (model: {matcher.model})")

    results = []
    total = len(unmatched)

    for source in unmatched["source_system"].unique():
        src_plants = unmatched[unmatched["source_system"] == source]
        if src_plants.empty:
            continue

        logger.info(f"LLM matching {len(src_plants):,} {source} plants...")

        # Build reference lists for candidate retrieval
        gem_names = load_gem(source)
        gem_name_list = list(gem_names.keys())

        cfg = SOURCE_COUNTRIES.get(source, {})
        gppd_countries = cfg.get("gppd_countries") or ([cfg["gppd"]] if cfg.get("gppd") else None)
        gppd_df = load_gppd(gppd_countries)
        gppd_raw_names = gppd_df["name"].dropna().unique().tolist()
        gppd_coords: dict[str, dict] = {}
        for _, grow in gppd_df.iterrows():
            n = grow["name"]
            if pd.notna(n) and n not in gppd_coords:
                gppd_coords[n] = {"lat": grow["latitude"], "lon": grow["longitude"]}

        # Build retriever
        retriever_sources: dict[str, list[str]] = {"GEM": gem_name_list}
        if gppd_raw_names:
            retriever_sources["GPPD"] = gppd_raw_names
        retriever = CandidateRetriever(retriever_sources)

        # All reference coords for resolving LLM matches
        all_coords: dict[str, dict[str, dict]] = {
            "GEM": {n: {"lat": info["lat"], "lon": info["lon"]} for n, info in gem_names.items()},
            "GPPD": gppd_coords,
        }

        for i, (_, row) in enumerate(src_plants.iterrows()):
            plant_name = row["plant_name"]
            if (i + 1) % 25 == 0:
                logger.info(f"  {source} LLM: {i + 1}/{len(src_plants)}")

            candidates_str = retriever.get_candidates(plant_name, limit=15)
            result = matcher.match(plant_name, candidates_str)

            if result.match and result.confidence in ("high", "medium"):
                # Parse "SOURCE: matched_name" from result.match
                ref_source = result.source
                matched_name = result.match
                # Strip source prefix if present
                for prefix in ("GEM: ", "GPPD: "):
                    if matched_name.startswith(prefix):
                        matched_name = matched_name[len(prefix):]
                        break

                # Look up coordinates
                coords = all_coords.get(ref_source, {}).get(matched_name, {})
                lat, lon = coords.get("lat"), coords.get("lon")

                if pd.notna(lat) and pd.notna(lon):
                    results.append({
                        "plant_name": plant_name,
                        "source_system": source,
                        "latitude": lat,
                        "longitude": lon,
                        "ref_source": ref_source or "LLM",
                        "matching_method": "llm",
                        "confidence": result.confidence,
                        "ref_matched_name": matched_name,
                    })

        logger.info(f"  {source} LLM: {len([r for r in results if r['source_system'] == source]):,} matched")

    return pd.DataFrame(results, columns=OUTPUT_COLUMNS) if results else pd.DataFrame(columns=OUTPUT_COLUMNS)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------
def build_unified_crosswalk(skip_llm: bool = False) -> pd.DataFrame:
    """Run the full pipeline and return the unified crosswalk DataFrame."""

    # Check for cached output
    if OUTPUT_FILE.exists():
        logger.info(f"Found existing output: {OUTPUT_FILE}")
        existing = pd.read_parquet(OUTPUT_FILE)
        logger.info(f"  {len(existing):,} rows, {existing['latitude'].notna().mean():.1%} with coords")
        logger.info("Delete the file to rebuild, or use --force to overwrite")
        return existing

    engine = _make_engine()

    # Step 1: Pull plant names
    logger.info("=" * 60)
    logger.info("Step 1: Pulling distinct plant names from Neon DB...")
    plants_df = pull_plant_names(engine)
    logger.info(f"Total distinct plant entries: {len(plants_df):,}")

    # Deduplicate (plant_name, source_system) — keep first (preserves OE lat/lon)
    plants_df = plants_df.drop_duplicates(subset=["plant_name", "source_system"], keep="first")
    logger.info(f"After dedup: {len(plants_df):,} unique (plant_name, source_system) pairs")

    # Step 3: Direct matching (OE)
    logger.info("=" * 60)
    logger.info("Step 3: Direct matching (OE embedded coords)...")
    exact_df = match_direct(plants_df)
    logger.info(f"Direct matches: {len(exact_df):,}")

    # Determine unmatched
    matched_keys = set(zip(exact_df["plant_name"], exact_df["source_system"])) if not exact_df.empty else set()
    unmatched_mask = ~plants_df.apply(lambda r: (r["plant_name"], r["source_system"]) in matched_keys, axis=1)
    unmatched_1 = plants_df[unmatched_mask]
    logger.info(f"Unmatched after exact: {len(unmatched_1):,}")

    # Step 4: Rapidfuzz matching
    logger.info("=" * 60)
    logger.info("Step 4: Rapidfuzz matching...")
    fuzzy_df = match_rapidfuzz(unmatched_1)
    logger.info(f"Rapidfuzz matches: {len(fuzzy_df):,}")

    # Update unmatched
    fuzzy_keys = set(zip(fuzzy_df["plant_name"], fuzzy_df["source_system"])) if not fuzzy_df.empty else set()
    all_matched = matched_keys | fuzzy_keys
    unmatched_mask_2 = ~plants_df.apply(lambda r: (r["plant_name"], r["source_system"]) in all_matched, axis=1)
    unmatched_2 = plants_df[unmatched_mask_2]
    logger.info(f"Unmatched after rapidfuzz: {len(unmatched_2):,}")

    # Step 5: LLM matching
    llm_df = pd.DataFrame(columns=OUTPUT_COLUMNS)
    if not skip_llm and not unmatched_2.empty:
        logger.info("=" * 60)
        logger.info("Step 5: LLM matching (Gemini)...")
        llm_df = match_llm(unmatched_2)
        logger.info(f"LLM matches: {len(llm_df):,}")
    elif skip_llm:
        logger.info("Step 5: Skipped (--no-llm flag)")

    # Step 6: Combine and save
    logger.info("=" * 60)
    logger.info("Step 6: Combining results...")

    # Build rows for still-unmatched plants (null coords)
    llm_keys = set(zip(llm_df["plant_name"], llm_df["source_system"])) if not llm_df.empty else set()
    final_matched = all_matched | llm_keys
    still_unmatched = plants_df[
        ~plants_df.apply(lambda r: (r["plant_name"], r["source_system"]) in final_matched, axis=1)
    ]

    unmatched_rows = []
    for _, row in still_unmatched.iterrows():
        unmatched_rows.append({
            "plant_name": row["plant_name"],
            "source_system": row["source_system"],
            "latitude": None,
            "longitude": None,
            "ref_source": None,
            "matching_method": None,
            "confidence": None,
            "ref_matched_name": None,
        })
    unmatched_df = pd.DataFrame(unmatched_rows, columns=OUTPUT_COLUMNS)

    unified = pd.concat([exact_df, fuzzy_df, llm_df, unmatched_df], ignore_index=True)

    # Save
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    unified.to_parquet(OUTPUT_FILE, index=False)
    logger.info(f"Saved {len(unified):,} rows to {OUTPUT_FILE}")

    # Summary
    logger.info("=" * 60)
    logger.info("Summary:")
    logger.info(f"  Total plants:    {len(unified):,}")
    coverage = unified["latitude"].notna().mean()
    logger.info(f"  With coords:     {unified['latitude'].notna().sum():,} ({coverage:.1%})")
    logger.info(f"  Without coords:  {unified['latitude'].isna().sum():,} ({1 - coverage:.1%})")
    logger.info(f"\n  By source_system:")
    for src in unified["source_system"].unique():
        subset = unified[unified["source_system"] == src]
        n = len(subset)
        cov = subset["latitude"].notna().mean()
        logger.info(f"    {src:8s}: {n:6,} plants, {cov:.1%} coverage")
    logger.info(f"\n  By matching_method:")
    for method, count in unified["matching_method"].value_counts(dropna=False).items():
        label = method if pd.notna(method) else "unmatched"
        logger.info(f"    {label:12s}: {count:,}")

    return unified


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Build unified plant coordinate crosswalk")
    parser.add_argument("--no-llm", action="store_true", help="Skip LLM matching step")
    parser.add_argument("--force", action="store_true", help="Overwrite existing output file")
    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="INFO")

    if args.force and OUTPUT_FILE.exists():
        OUTPUT_FILE.unlink()
        logger.info(f"Removed existing output: {OUTPUT_FILE}")

    build_unified_crosswalk(skip_llm=args.no_llm)


if __name__ == "__main__":
    main()

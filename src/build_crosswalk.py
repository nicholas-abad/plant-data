#!/usr/bin/env python3
"""Build unified plant coordinate crosswalk from all 6 generation sources.

Produces a single parquet file mapping every unique plant name (from EIA,
ENTSOE, NPP, ONS, OE, OCCTO) to coordinates, with an audit trail of how each
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
    python -m src.build_crosswalk                       # run full pipeline
    python -m src.build_crosswalk --no-llm              # skip LLM step
    python -m src.build_crosswalk --force               # rebuild from scratch
    python -m src.build_crosswalk --sources OCCTO       # run only for OCCTO (appends to existing)
    python -m src.build_crosswalk --sources OCCTO NPP   # run for specific sources
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

GEM_CSV = get_crosswalk_dir() / "GEM database_21Feb2026.csv"
GPPD_CSV = get_crosswalk_dir() / "global_power_plant_database.csv"
EIA_LOOKUP_CSV = get_crosswalk_dir() / "eia_plant_lookup.csv"

# Rapidfuzz thresholds (same as notebook / dashboard)
GEM_THRESHOLD = 80
GPPD_THRESHOLD = 80

# Country filters for each source when querying GPPD / GEM
SOURCE_COUNTRIES = {
    "NPP": {"gppd": "IND", "gem": "India"},
    "ENTSOE": {
        "gppd_countries": [
            "AUT", "BEL", "BGR", "HRV", "CZE", "DNK", "EST", "FIN", "FRA",
            "DEU", "GRC", "HUN", "IRL", "ITA", "LVA", "LTU", "LUX", "NLD",
            "POL", "PRT", "ROU", "SVK", "SVN", "ESP", "SWE", "GBR", "NOR",
            "CHE", "SRB", "BIH", "MNE", "MKD", "ALB", "XKX",
        ],
        "gem_countries": [
            "Albania", "Austria", "Belgium", "Bosnia and Herzegovina",
            "Bulgaria", "Croatia", "Czech Republic", "Denmark", "Estonia",
            "Finland", "France", "Germany", "Greece", "Hungary", "Ireland",
            "Italy", "Kosovo", "Latvia", "Lithuania", "Luxembourg",
            "Montenegro", "Netherlands", "North Macedonia", "Norway",
            "Poland", "Portugal", "Romania", "Serbia", "Slovakia",
            "Slovenia", "Spain", "Sweden", "Switzerland", "United Kingdom",
        ],
    },
    "EIA": {"gppd": "USA", "gem": "United States of America"},
    "ONS": {"gppd": "BRA", "gem": "Brazil"},
    "OE": {"gppd": "AUS", "gem": "Australia"},
    "OCCTO": {"gppd": "JPN", "gem": "Japan"},
}

# Columns in the output
OUTPUT_COLUMNS = [
    "plant_name", "plant_code", "source_system", "latitude", "longitude",
    "ref_source", "matching_method", "confidence", "ref_matched_name", "reasoning",
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
def pull_plant_names(engine, sources: list[str] | None = None) -> pd.DataFrame:
    """Pull distinct plant names from generation tables.

    Args:
        engine: SQLAlchemy engine.
        sources: If provided, only pull from these source systems.
                 If None, pull from all sources.
    """
    all_queries = {
        "NPP": "SELECT DISTINCT plant AS plant_name FROM npp_generation WHERE plant IS NOT NULL",
        "ENTSOE": "SELECT DISTINCT plant_name FROM entsoe_generation_data WHERE plant_name IS NOT NULL",
        "EIA": "SELECT DISTINCT plant_code AS plant_name FROM eia_generation_data WHERE plant_code IS NOT NULL",
        "ONS": "SELECT DISTINCT plant AS plant_name FROM ons_generation_data WHERE plant IS NOT NULL",
        "OE": "SELECT DISTINCT facility_name AS plant_name, latitude, longitude FROM oe_facility_generation_data WHERE facility_name IS NOT NULL",
        "OCCTO": "SELECT DISTINCT plant AS plant_name FROM occto_generation_data WHERE plant IS NOT NULL",
    }

    queries = {k: v for k, v in all_queries.items() if sources is None or k in sources}

    frames = []
    with engine.connect() as conn:
        conn.execute(text("SET statement_timeout = '120s'"))
        for source, sql in queries.items():
            logger.info(f"Pulling {source} plant names...")
            df = pd.read_sql(text(sql), conn)
            df["source_system"] = source
            frames.append(df)
            logger.info(f"  {source}: {len(df):,} distinct plants")

    # EIA: resolve plant_code → plant_name via lookup CSV
    eia_indices = [i for i, (src, _) in enumerate(queries.items()) if src == "EIA"]
    if eia_indices:
        eia_idx = eia_indices[0]
        eia_df = frames[eia_idx]
        if EIA_LOOKUP_CSV.exists():
            lookup = pd.read_csv(EIA_LOOKUP_CSV, dtype={"plant_code": str})
            eia_df = eia_df.rename(columns={"plant_name": "plant_code"})
            eia_df["plant_code"] = eia_df["plant_code"].astype(str)
            eia_df = eia_df.merge(lookup, on="plant_code", how="left")
            eia_df["plant_name"] = eia_df["plant_name"].fillna(eia_df["plant_code"])
            logger.info(f"  EIA: resolved {eia_df['plant_name'].ne(eia_df['plant_code']).sum():,} plant codes to names via lookup")
        else:
            eia_df["plant_code"] = eia_df["plant_name"]
            logger.warning(f"  EIA lookup CSV not found: {EIA_LOOKUP_CSV}, using plant_code as plant_name")
        frames[eia_idx] = eia_df

    # Add plant_code=None for non-EIA sources
    for i, f in enumerate(frames):
        if "plant_code" not in f.columns:
            f["plant_code"] = None
            frames[i] = f

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
        gem_countries = cfg.get("gem_countries")
        gem_country = cfg.get("gem")
        if gem_countries:
            gem_raw = gem_raw[gem_raw["Country/Area"].isin(gem_countries)]
        elif gem_country:
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
                    "plant_code": row.get("plant_code"),
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
    ref_sources: list[str] | None = None,
) -> pd.DataFrame:
    """Rapidfuzz matching against GEM and/or GPPD.

    Args:
        unmatched: DataFrame of plants to match.
        ref_sources: Which reference DBs to check (e.g. ["GEM"], ["GPPD"],
                     or None for both). Default None = both.
    """
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
            if gem_norm_list and (ref_sources is None or "GEM" in ref_sources):
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
                            "plant_code": row.get("plant_code"),
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
            if not matched and gppd_norm_list and (ref_sources is None or "GPPD" in ref_sources):
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
                            "plant_code": row.get("plant_code"),
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

        # For cross-language sources (e.g., OCCTO: Japanese kanji vs English),
        # bypass fuzzy retrieval and give the LLM all candidates at once.
        if source == "OCCTO":
            all_candidates_str = retriever.get_all_candidates()

        # All reference coords for resolving LLM matches
        all_coords: dict[str, dict[str, dict]] = {
            "GEM": {n: {"lat": info["lat"], "lon": info["lon"]} for n, info in gem_names.items()},
            "GPPD": gppd_coords,
        }

        for i, (_, row) in enumerate(src_plants.iterrows()):
            plant_name = row["plant_name"]
            if (i + 1) % 25 == 0:
                logger.info(f"  {source} LLM: {i + 1}/{len(src_plants)}")

            if source == "OCCTO":
                candidates_str = all_candidates_str
            else:
                candidates_str = retriever.get_candidates(plant_name, limit=15)
            result = matcher.match(plant_name, candidates_str, source_system=source)

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
                        "plant_code": row.get("plant_code"),
                        "source_system": source,
                        "latitude": lat,
                        "longitude": lon,
                        "ref_source": ref_source or "LLM",
                        "matching_method": "llm",
                        "confidence": result.confidence,
                        "ref_matched_name": matched_name,
                        "reasoning": result.reasoning,
                    })

        logger.info(f"  {source} LLM: {len([r for r in results if r['source_system'] == source]):,} matched")

    return pd.DataFrame(results, columns=OUTPUT_COLUMNS) if results else pd.DataFrame(columns=OUTPUT_COLUMNS)


def _log_per_source(matched_df: pd.DataFrame, input_df: pd.DataFrame, stage: str):
    """Log per-source breakdown for a matching stage."""
    for src in input_df["source_system"].unique():
        src_total = len(input_df[input_df["source_system"] == src])
        src_matched = len(matched_df[matched_df["source_system"] == src]) if not matched_df.empty else 0
        pct = src_matched / src_total if src_total > 0 else 0
        logger.info(f"    {src:8s}: {src_matched:,}/{src_total:,} ({pct:.1%})")


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------
def build_unified_crosswalk(
    skip_llm: bool = False,
    sources: list[str] | None = None,
    yes: bool = False,
) -> pd.DataFrame:
    """Run the full pipeline and return the unified crosswalk DataFrame.

    Args:
        skip_llm: If True, skip the LLM matching step.
        sources: If provided, only process these source systems.
                 Results are merged into any existing crosswalk file.
                 If None, process all sources (full rebuild).
        yes: If True, skip interactive confirmations.
    """
    existing = None

    # When running for specific sources, load existing and merge later
    if sources and OUTPUT_FILE.exists():
        existing = pd.read_parquet(OUTPUT_FILE)
        logger.info(f"Loaded existing crosswalk: {len(existing):,} rows")
        # Remove old rows for the requested sources (we'll rebuild them)
        existing = existing[~existing["source_system"].isin(sources)]
        logger.info(f"  Kept {len(existing):,} rows (excluded {', '.join(sources)} for rebuild)")
    elif not sources and OUTPUT_FILE.exists():
        logger.info(f"Found existing output: {OUTPUT_FILE}")
        cached = pd.read_parquet(OUTPUT_FILE)
        logger.info(f"  {len(cached):,} rows, {cached['latitude'].notna().mean():.1%} with coords")
        logger.info("Delete the file to rebuild, or use --force to overwrite")
        return cached

    engine = _make_engine()

    # Step 1: Pull plant names
    logger.info("=" * 60)
    src_label = ", ".join(sources) if sources else "all"
    logger.info(f"Step 1: Pulling distinct plant names from Neon DB ({src_label})...")
    plants_df = pull_plant_names(engine, sources=sources)
    logger.info(f"Total distinct plant entries: {len(plants_df):,}")

    # Save full EIA plant_code→plant_name mapping before dedup
    # (multiple plant_codes can share the same name; we expand back after matching)
    eia_code_map = plants_df.loc[
        plants_df["source_system"] == "EIA", ["plant_name", "plant_code"]
    ].copy()

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

    # Step 4: Rapidfuzz matching (GEM)
    logger.info("=" * 60)
    logger.info("Step 4: Rapidfuzz matching (GEM)...")
    gem_df = match_rapidfuzz(unmatched_1, ref_sources=["GEM"])
    logger.info(f"GEM matches: {len(gem_df):,}")
    _log_per_source(gem_df, unmatched_1, "GEM rapidfuzz")

    # Update unmatched after GEM
    gem_keys = set(zip(gem_df["plant_name"], gem_df["source_system"])) if not gem_df.empty else set()
    all_matched_gem = matched_keys | gem_keys
    unmatched_after_gem = plants_df[
        ~plants_df.apply(lambda r: (r["plant_name"], r["source_system"]) in all_matched_gem, axis=1)
    ]
    logger.info(f"Unmatched after GEM: {len(unmatched_after_gem):,}")

    # Step 5: Rapidfuzz matching (GPPD)
    logger.info("=" * 60)
    logger.info("Step 5: Rapidfuzz matching (GPPD)...")
    gppd_df = match_rapidfuzz(unmatched_after_gem, ref_sources=["GPPD"])
    logger.info(f"GPPD matches: {len(gppd_df):,}")
    _log_per_source(gppd_df, unmatched_after_gem, "GPPD rapidfuzz")

    # Update unmatched after GPPD
    gppd_keys = set(zip(gppd_df["plant_name"], gppd_df["source_system"])) if not gppd_df.empty else set()
    all_matched = all_matched_gem | gppd_keys
    unmatched_2 = plants_df[
        ~plants_df.apply(lambda r: (r["plant_name"], r["source_system"]) in all_matched, axis=1)
    ]
    logger.info(f"Unmatched after GEM+GPPD: {len(unmatched_2):,}")

    # Step 6: LLM matching
    llm_df = pd.DataFrame(columns=OUTPUT_COLUMNS)
    if not skip_llm and not unmatched_2.empty:
        logger.info("=" * 60)
        logger.info("Step 6: LLM matching (Gemini)...")

        n_plants = len(unmatched_2)
        est_cost = n_plants * 0.001  # rough estimate: ~$0.001 per plant
        logger.info(f"LLM matching will process {n_plants:,} plants")
        logger.info(f"Estimated cost: ~${est_cost:.2f}")
        if yes:
            confirm = "y"
        else:
            confirm = input(f"Proceed with LLM matching for {n_plants:,} plants (~${est_cost:.2f})? [y/N] ")
        if confirm.strip().lower() == "y":
            llm_df = match_llm(unmatched_2)
            logger.info(f"LLM matches: {len(llm_df):,}")
            _log_per_source(llm_df, unmatched_2, "LLM")
        else:
            logger.info("LLM matching skipped by user")
    elif skip_llm:
        logger.info("Step 6: Skipped (--no-llm flag)")

    # Step 7: Combine and save
    logger.info("=" * 60)
    logger.info("Step 7: Combining results...")

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
            "plant_code": row.get("plant_code"),
            "source_system": row["source_system"],
            "latitude": None,
            "longitude": None,
            "ref_source": None,
            "matching_method": None,
            "confidence": None,
            "ref_matched_name": None,
            "reasoning": None,
        })
    unmatched_df = pd.DataFrame(unmatched_rows, columns=OUTPUT_COLUMNS)

    new_rows = pd.concat([exact_df, gem_df, gppd_df, llm_df, unmatched_df], ignore_index=True)

    # Expand EIA rows: if multiple plant_codes share the same plant_name,
    # create one crosswalk row per plant_code (all sharing the same coords)
    eia_rows = new_rows[new_rows["source_system"] == "EIA"]
    non_eia_rows = new_rows[new_rows["source_system"] != "EIA"]
    if not eia_rows.empty and not eia_code_map.empty:
        # Drop the single plant_code from matching, re-join with full mapping
        eia_expanded = eia_rows.drop(columns=["plant_code"]).merge(
            eia_code_map, on="plant_name", how="left",
        )
        new_rows = pd.concat([non_eia_rows, eia_expanded], ignore_index=True)
        n_added = len(new_rows) - len(non_eia_rows) - len(eia_rows)
        if n_added > 0:
            logger.info(f"Expanded {n_added} additional EIA rows for duplicate plant names")

    # Merge with existing crosswalk when running for specific sources
    if existing is not None:
        unified = pd.concat([existing, new_rows], ignore_index=True)
        logger.info(f"Merged {len(new_rows):,} new rows with {len(existing):,} existing → {len(unified):,} total")
    else:
        unified = new_rows

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

    valid_sources = list(SOURCE_COUNTRIES.keys())

    parser = argparse.ArgumentParser(description="Build unified plant coordinate crosswalk")
    parser.add_argument("--no-llm", action="store_true", help="Skip LLM matching step")
    parser.add_argument("--force", action="store_true", help="Overwrite existing output file")
    parser.add_argument(
        "--sources", nargs="+", choices=valid_sources, metavar="SOURCE",
        help=f"Only process specific sources (appends to existing). Choices: {', '.join(valid_sources)}",
    )
    parser.add_argument("--yes", "-y", action="store_true", help="Skip interactive confirmations")
    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="INFO")

    if args.force and not args.sources and OUTPUT_FILE.exists():
        OUTPUT_FILE.unlink()
        logger.info(f"Removed existing output: {OUTPUT_FILE}")

    build_unified_crosswalk(skip_llm=args.no_llm, sources=args.sources, yes=args.yes)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Test LLM matching on NPP (India) plants only.

Runs rapidfuzz + LLM matching for NPP plants and prints a before/after
coverage comparison. Saves results to a separate test parquet so it
doesn't overwrite the production crosswalk.

Usage:
    cd data/plant-data
    python scripts/test_npp_llm.py
"""

import os
import sys

import pandas as pd
from dotenv import load_dotenv
from loguru import logger
from sqlalchemy import create_engine, text

# Add package root to path so we can import src modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.build_crosswalk import (
    OUTPUT_COLUMNS,
    match_llm,
    match_rapidfuzz,
)
from src.utils import get_crosswalk_dir

OUTPUT_FILE = get_crosswalk_dir() / "npp_llm_test.parquet"


def _make_engine():
    """Create SQLAlchemy engine from environment variables."""
    load_dotenv()
    url = os.environ.get("DATABASE_URL")
    if url:
        return create_engine(url, connect_args={"connect_timeout": 30})
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


def pull_npp_plant_names(engine) -> pd.DataFrame:
    """Pull distinct NPP plant names from Neon."""
    sql = "SELECT DISTINCT plant AS plant_name FROM npp_generation WHERE plant IS NOT NULL"
    with engine.connect() as conn:
        conn.execute(text("SET statement_timeout = '120s'"))
        logger.info("Pulling NPP plant names...")
        df = pd.read_sql(text(sql), conn)
        df["source_system"] = "NPP"
        logger.info(f"  NPP: {len(df):,} distinct plants")
    return df


def main():
    logger.remove()
    logger.add(sys.stderr, level="INFO")

    engine = _make_engine()

    # Step 1: Pull NPP plant names
    logger.info("=" * 60)
    logger.info("Step 1: Pulling NPP plant names from Neon DB...")
    plants_df = pull_npp_plant_names(engine)
    plants_df = plants_df.drop_duplicates(subset=["plant_name", "source_system"], keep="first")
    total_plants = len(plants_df)
    logger.info(f"Total NPP plants: {total_plants:,}")

    # Step 2: Rapidfuzz matching with GEM
    logger.info("=" * 60)
    logger.info("Step 2: Rapidfuzz matching (GEM)...")
    gem_df = match_rapidfuzz(plants_df, ref_sources=["GEM"])
    logger.info(f"GEM matches: {len(gem_df):,} ({len(gem_df)/total_plants:.1%})")

    # Determine unmatched after GEM
    gem_keys = set(zip(gem_df["plant_name"], gem_df["source_system"])) if not gem_df.empty else set()
    unmatched_after_gem = plants_df[~plants_df.apply(lambda r: (r["plant_name"], r["source_system"]) in gem_keys, axis=1)]
    logger.info(f"Unmatched after GEM: {len(unmatched_after_gem):,}")

    # Step 3: Rapidfuzz matching with GPPD (on remaining unmatched)
    logger.info("=" * 60)
    logger.info("Step 3: Rapidfuzz matching (GPPD)...")
    gppd_df = match_rapidfuzz(unmatched_after_gem, ref_sources=["GPPD"])
    logger.info(f"GPPD matches: {len(gppd_df):,} ({len(gppd_df)/total_plants:.1%})")

    # Determine unmatched after GPPD
    gppd_keys = set(zip(gppd_df["plant_name"], gppd_df["source_system"])) if not gppd_df.empty else set()
    all_fuzzy_keys = gem_keys | gppd_keys
    unmatched = plants_df[~plants_df.apply(lambda r: (r["plant_name"], r["source_system"]) in all_fuzzy_keys, axis=1)]
    logger.info(f"Unmatched after GEM+GPPD: {len(unmatched):,}")

    if unmatched.empty:
        logger.info("All plants matched by rapidfuzz — nothing for LLM to do!")
        _save_and_summarize(plants_df, gem_df, gppd_df, pd.DataFrame(columns=OUTPUT_COLUMNS), total_plants)
        return

    # Step 4: Cost confirmation + LLM matching
    logger.info("=" * 60)
    logger.info("Step 4: LLM matching (Gemini)...")

    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        logger.error("GEMINI_API_KEY not set — cannot run LLM matching")
        sys.exit(1)

    n_plants = len(unmatched)
    est_cost = n_plants * 0.001  # rough estimate: ~$0.001 per plant
    logger.info(f"LLM matching will process {n_plants:,} plants")
    logger.info(f"Estimated cost: ~${est_cost:.2f}")
    confirm = input(f"Proceed with LLM matching for {n_plants:,} plants (~${est_cost:.2f})? [y/N] ")
    if confirm.strip().lower() != "y":
        logger.info("LLM matching skipped by user")
        _save_and_summarize(plants_df, gem_df, gppd_df, pd.DataFrame(columns=OUTPUT_COLUMNS), total_plants)
        return

    llm_df = match_llm(unmatched)
    logger.info(f"LLM matches: {len(llm_df):,}")

    # Save and summarize
    _save_and_summarize(plants_df, gem_df, gppd_df, llm_df, total_plants)


def _save_and_summarize(
    plants_df: pd.DataFrame,
    gem_df: pd.DataFrame,
    gppd_df: pd.DataFrame,
    llm_df: pd.DataFrame,
    total_plants: int,
):
    """Save test results and print 4-stage coverage comparison."""
    # Build unmatched rows
    matched_keys = set()
    for df in (gem_df, gppd_df, llm_df):
        if not df.empty:
            matched_keys |= set(zip(df["plant_name"], df["source_system"]))

    still_unmatched = plants_df[
        ~plants_df.apply(lambda r: (r["plant_name"], r["source_system"]) in matched_keys, axis=1)
    ]
    unmatched_rows = []
    for _, row in still_unmatched.iterrows():
        unmatched_rows.append({
            "plant_name": row["plant_name"],
            "source_system": "NPP",
            "latitude": None,
            "longitude": None,
            "ref_source": None,
            "matching_method": None,
            "confidence": None,
            "ref_matched_name": None,
            "reasoning": None,
        })
    unmatched_df = pd.DataFrame(unmatched_rows, columns=OUTPUT_COLUMNS)

    combined = pd.concat([gem_df, gppd_df, llm_df, unmatched_df], ignore_index=True)

    # Save
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(OUTPUT_FILE, index=False)
    logger.info(f"Saved {len(combined):,} rows to {OUTPUT_FILE}")

    # Coverage comparison
    gem_matched = len(gem_df)
    gppd_matched = len(gppd_df)
    llm_matched = len(llm_df)
    fuzzy_total = gem_matched + gppd_matched
    total_matched = fuzzy_total + llm_matched
    total_unmatched = total_plants - total_matched

    logger.info("=" * 60)
    logger.info("Coverage Comparison (NPP)")
    logger.info("=" * 60)
    logger.info(f"  Total NPP plants:           {total_plants:,}")
    logger.info(f"  After GEM rapidfuzz:         {gem_matched:,} matched ({gem_matched/total_plants:.1%})")
    logger.info(f"  After GPPD rapidfuzz:        +{gppd_matched:,} matched ({(gem_matched + gppd_matched)/total_plants:.1%} cumulative)")
    logger.info(f"    Unmatched after rapidfuzz:  {total_plants - fuzzy_total:,} ({(total_plants - fuzzy_total)/total_plants:.1%})")
    logger.info(f"  After LLM:                   +{llm_matched:,} matched ({total_matched/total_plants:.1%} cumulative)")
    logger.info(f"    Still unmatched:           {total_unmatched:,} ({total_unmatched/total_plants:.1%})")
    logger.info(f"  LLM improvement:             +{(llm_matched/total_plants):.1%} coverage")

    if not llm_df.empty:
        logger.info(f"\n  LLM match confidence breakdown:")
        for conf, count in llm_df["confidence"].value_counts().items():
            logger.info(f"    {conf}: {count:,}")


if __name__ == "__main__":
    main()

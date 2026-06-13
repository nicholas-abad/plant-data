#!/usr/bin/env python3
"""Build unified plant coordinate crosswalk from all generation sources.

Produces a single parquet file mapping every unique plant name (from EIA,
ENTSOE, NPP, ONS, OE, OCCTO, CHILE) to coordinates, with an audit trail of
how each was matched.

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
    python -m src.build_crosswalk --sources CHILE       # run only for CHILE (appends to existing)
    python -m src.build_crosswalk --sources OCCTO NPP   # run for specific sources
"""

import os
import sys

import pandas as pd
from dotenv import load_dotenv
from loguru import logger
from rapidfuzz import fuzz, process
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

from .plant_name_matchers import (
    CandidateRetriever,
    GeminiNameMatcher,
    build_norm_index,
    normalize_for_comparison,
    normalize_gppd_name,
    validate_match,
)
from .utils import get_crosswalk_dir, validate_coordinates

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
# Fuzzy hits at/above this score are trusted outright; only the marginal
# band (THRESHOLD..TRUST) is additionally gated by validate_match. Exact
# containment as a hard gate on ALL hits rejected true one-letter
# transliteration variants ("Vindhyachal"/"Vindhyanchal" scores ~96) —
# while the documented false positives score in the 80s ("BHADRA HPS" →
# "Bhandara power station" ≈ 86) and stay guarded.
VALIDATE_TRUST_SCORE = 90

# Country filters for each source when querying GPPD / GEM
SOURCE_COUNTRIES = {
    "NPP": {"gppd": "IND", "gem": "India"},
    "ENTSOE": {
        "gppd_countries": [
            "AUT",
            "BEL",
            "BGR",
            "HRV",
            "CZE",
            "DNK",
            "EST",
            "FIN",
            "FRA",
            "DEU",
            "GRC",
            "HUN",
            "IRL",
            "ITA",
            "LVA",
            "LTU",
            "LUX",
            "NLD",
            "POL",
            "PRT",
            "ROU",
            "SVK",
            "SVN",
            "ESP",
            "SWE",
            "GBR",
            "NOR",
            "CHE",
            "SRB",
            "BIH",
            "MNE",
            "MKD",
            "ALB",
            "XKX",
        ],
        "gem_countries": [
            "Albania",
            "Austria",
            "Belgium",
            "Bosnia and Herzegovina",
            "Bulgaria",
            "Croatia",
            "Czech Republic",
            "Denmark",
            "Estonia",
            "Finland",
            "France",
            "Germany",
            "Greece",
            "Hungary",
            "Ireland",
            "Italy",
            "Kosovo",
            "Latvia",
            "Lithuania",
            "Luxembourg",
            "Montenegro",
            "Netherlands",
            "North Macedonia",
            "Norway",
            "Poland",
            "Portugal",
            "Romania",
            "Serbia",
            "Slovakia",
            "Slovenia",
            "Spain",
            "Sweden",
            "Switzerland",
            "United Kingdom",
        ],
    },
    "EIA": {"gppd": "USA", "gem": "United States"},
    "ONS": {"gppd": "BRA", "gem": "Brazil"},
    "OE": {"gppd": "AUS", "gem": "Australia"},
    "OCCTO": {"gppd": "JPN", "gem": "Japan"},
    "CHILE": {"gppd": "CHL", "gem": "Chile"},
}

# Columns in the output
OUTPUT_COLUMNS = [
    "plant_name",
    "plant_code",
    "source_system",
    "latitude",
    "longitude",
    "ref_source",
    "matching_method",
    "confidence",
    "ref_matched_name",
    "reasoning",
    "coal_type",
    "combustion_tech",
    "capacity_mw",
    "state",
    "sector",
]

NPP_GIPT_CSV = get_crosswalk_dir() / "NPP_GIPT_crosswalk (1).csv"


def _parse_gem_capacity(val) -> float | None:
    """Parse GEM `Capacity` values like '660.0 MW' → float MW. None if unparseable."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val) if pd.notna(val) else None
    if not isinstance(val, str):
        return None
    s = val.strip().lower().replace("mw", "").strip()
    try:
        return float(s) if s else None
    except ValueError:
        return None


# NPP plant names contain technology suffixes that reveal whether they're coal,
# hydro, gas, nuclear, etc. The crosswalk's GEM matcher pulls coal-fuel rows from
# GEM and attaches their `capacity_mw` / `coal_type` / `combustion_tech` to any
# matching plant — including NPP hydro/gas plants that fuzzy-match to a coal
# plant with a similar name (e.g. "BHADRA HPS" → "Bhandara power station").
# This regex catches the obvious non-coal NPP suffixes so we can suppress coal
# metadata attribution.
import re as _re_npp  # noqa: E402  # placed here to keep the regex co-located with the docstring above

_NPP_NON_COAL_SUFFIX = _re_npp.compile(
    r"(?:^|[\s\W])(?:HPS|HEP|HEPP|CCPP|OCGT|CCGT|GT-?\d|NUCLEAR|NPP|"
    r"WIND|SOLAR|PV|HYDRO|HYDEL|RES)(?:$|[\s\W])",
    _re_npp.IGNORECASE,
)


def _is_npp_likely_non_coal(plant_name) -> bool:
    """True when an NPP plant's name has a non-coal technology suffix."""
    if not isinstance(plant_name, str):
        return False
    return bool(_NPP_NON_COAL_SUFFIX.search(plant_name))


_LLM_SCORE_SUFFIX = _re_npp.compile(r"\s*\(score:\s*\d+(?:\.\d+)?\)\s*$")


def _clean_llm_match(match: str) -> tuple[str | None, str]:
    """Split an LLM match like 'GEM: Foo power station (score: 95)'.

    Returns (source_from_prefix, cleaned_name). The prefix is authoritative
    for the reference source (the model's separate `source` field sometimes
    says 'Crosswalk' or differs in case); the '(score: N)' suffix is echoed
    candidate formatting, not part of the plant name — both used to cause
    silent coordinate-lookup misses.
    """
    source = None
    name = match.strip()
    for prefix in ("GEM: ", "GPPD: "):
        if name.startswith(prefix):
            source = prefix[:-2]
            name = name[len(prefix) :]
            break
    name = _LLM_SCORE_SUFFIX.sub("", name).strip()
    return source, name


def _normalize_confidence(confidence) -> str | None:
    """Lowercase free-form LLM confidence ('High' → 'high')."""
    return confidence.strip().lower() if isinstance(confidence, str) else None


def _norm_npp_name(name) -> str:
    """Whitespace/case-insensitive key for matching DGR plant names.

    The manually-curated NPP_GIPT crosswalk carries irregular spacing in
    `DGR plant name` (e.g. 'BARH  STPS', leading/trailing spaces) while the
    extractor stores names with collapsed/stripped whitespace. Matching on a
    normalized key recovers those plants (worth ~1.5-2% of recent India coal,
    growing over time as plants like BARH STPS / NTPL TUTICORIN ramp up).
    """
    return _re_npp.sub(r"\s+", " ", str(name)).strip().lower()


def _parse_gem_coal_type(fuel_value) -> str | None:
    """Parse GEM `Fuel` field for a coal-only plant → coal_type token.

    GEM's `Fuel` is a comma-separated list like "coal: bituminous" or
    "natural gas, industrial by-product: blast furnace gas". We only return
    a coal_type when the *first* token is a coal entry — multi-fuel plants
    and non-coal plants return None.

    Returns lowercase coal_type ("bituminous", "lignite", ...) or None.
    """
    if not isinstance(fuel_value, str):
        return None
    s = fuel_value.strip().lower()
    # Only first fuel counts; multi-fuel plants return None
    first = s.split(",", 1)[0].strip()
    if not first.startswith("coal"):
        return None
    if ":" in first:
        subtype = first.split(":", 1)[1].strip()
    else:
        subtype = ""
    if not subtype or subtype == "unknown":
        return None
    if subtype == "waste coal":
        return "waste"
    return subtype


def _is_gem_coal_row(fuel_value) -> bool:
    if not isinstance(fuel_value, str):
        return False
    return fuel_value.strip().lower().startswith("coal")


def _normalize_combustion_tech(tech_value) -> str | None:
    """Normalize GEM `Technology` value to canonical forms used by the dashboard.

    Canonical: subcritical, supercritical, ultra-supercritical, CFB, IGCC.
    Returns None for unknown/missing/non-coal tech (e.g. gas turbine).
    """
    if not isinstance(tech_value, str):
        return None
    s = tech_value.strip()
    if not s or s.lower() == "unknown":
        return None
    low = s.lower().replace("-", "").replace(" ", "")
    if low == "subcritical":
        return "subcritical"
    if low == "supercritical":
        return "supercritical"
    if low in ("ultrasupercritical", "usc"):
        return "ultra-supercritical"
    if low == "cfb":
        return "CFB"
    if low == "igcc":
        return "IGCC"
    # Non-coal combustion techs (gas turbine, combined cycle, etc.) → None
    return None


def _make_engine():
    """Create SQLAlchemy engine from environment variables."""
    load_dotenv()
    url = os.environ.get("DATABASE_URL")
    if url:
        return create_engine(url, connect_args={"connect_timeout": 30})
    # Fall back to individual env vars
    ssl = os.environ.get("POSTGRES_SSLMODE", "require")
    connection_url = URL.create(
        drivername="postgresql",
        username=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        database=os.environ["POSTGRES_DB"],
        query={"sslmode": ssl} if ssl else {},
    )
    return create_engine(connection_url, connect_args={"connect_timeout": 30})


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
        # mv instead of the raw table: the DISTINCT scan over 60M+ raw rows
        # takes minutes on a cold Neon cache (observed: 6m14s, TCP-timing out
        # the rebuild); the 55k-row mat view has the identical plant set
        # (verified count-equal) and is refreshed by the ETL after every load.
        "ENTSOE": "SELECT DISTINCT plant_name FROM mv_entsoe_plant_monthly WHERE plant_name IS NOT NULL",
        "EIA": "SELECT DISTINCT plant_code AS plant_name FROM eia_generation_data WHERE plant_code IS NOT NULL",
        "ONS": "SELECT DISTINCT plant AS plant_name FROM ons_generation_data WHERE plant IS NOT NULL",
        "OE": "SELECT DISTINCT facility_name AS plant_name, latitude, longitude FROM oe_facility_generation_data WHERE facility_name IS NOT NULL",
        "OCCTO": "SELECT DISTINCT plant AS plant_name FROM occto_generation_data WHERE plant IS NOT NULL",
        "CHILE": "SELECT DISTINCT plant AS plant_name FROM chile_generation_data WHERE plant IS NOT NULL",
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
        if not EIA_LOOKUP_CSV.exists():
            raise FileNotFoundError(
                f"EIA plant lookup not found: {EIA_LOOKUP_CSV}\n"
                "  This file is required to map plant_code → plant_name for EIA "
                "records. Without it, the crosswalk would contain plant codes "
                "(e.g. '12345') in place of human-readable names "
                "(e.g. 'Smith Power Plant'), corrupting downstream matching.\n"
                "  Generate the lookup, or re-run with `--sources NPP ENTSOE ONS "
                "OE OCCTO` to skip EIA."
            )
        lookup = pd.read_csv(EIA_LOOKUP_CSV, dtype={"plant_code": str})
        eia_df = eia_df.rename(columns={"plant_name": "plant_code"})
        eia_df["plant_code"] = eia_df["plant_code"].astype(str)
        eia_df = eia_df.merge(lookup, on="plant_code", how="left")
        eia_df["plant_name"] = eia_df["plant_name"].fillna(eia_df["plant_code"])
        logger.info(
            f"  EIA: resolved {eia_df['plant_name'].ne(eia_df['plant_code']).sum():,} plant codes to names via lookup"
        )
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
        if pd.isna(name):
            continue
        is_coal = _is_gem_coal_row(row.get("Fuel"))
        cap = _parse_gem_capacity(row.get("Capacity")) if is_coal else None
        info = {
            "lat": row["Latitude"],
            "lon": row["Longitude"],
            "name": name,
            "coal_type": _parse_gem_coal_type(row.get("Fuel")) if is_coal else None,
            "combustion_tech": _normalize_combustion_tech(row.get("Technology"))
            if is_coal
            else None,
            "capacity_mw": cap,
            "_is_coal": is_coal,
        }
        existing = names.get(name)
        if existing is None:
            names[name] = info
        elif is_coal and not existing.get("_is_coal"):
            # Coal entry replaces non-coal first-wins
            names[name] = info
        elif is_coal and existing.get("_is_coal"):
            # Both coal: sum capacity across units; keep first coords/tech/type
            if cap is not None:
                existing["capacity_mw"] = (existing.get("capacity_mw") or 0.0) + cap
    return names


def load_gppd(country_codes: list[str] | None = None) -> pd.DataFrame:
    """Load GPPD entries from local CSV, optionally filtered by country."""
    if not GPPD_CSV.exists():
        logger.warning(f"GPPD CSV not found: {GPPD_CSV}")
        return pd.DataFrame(columns=["name", "latitude", "longitude", "country"])

    gppd = pd.read_csv(
        GPPD_CSV, usecols=["name", "latitude", "longitude", "country"], low_memory=False
    )
    if country_codes:
        gppd = gppd[gppd["country"].isin(country_codes)]
    return gppd


# ---------------------------------------------------------------------------
# Step 3: Direct matching (OE embedded coordinates)
# ---------------------------------------------------------------------------
def match_npp_via_gipt(plants_df: pd.DataFrame) -> pd.DataFrame:
    """Authoritative NPP plant matching via the manually-curated NPP_GIPT crosswalk.

    Each row in the crosswalk maps an NPP plant-unit to a GEM unit/phase ID.
    For coal plants we look up each unit in GEM, sum unit-level capacities to
    get plant-level capacity_mw, and pull lat/lon + coal_type + combustion_tech
    from GEM. State and Sector come from the crosswalk itself.

    Only `Type == "coal"` rows produce coal-metadata; non-coal NPP plants in
    the crosswalk are skipped so they fall through to fuzzy/LLM matching for
    coordinates only.
    """
    if not NPP_GIPT_CSV.exists():
        logger.warning(f"NPP_GIPT crosswalk not found: {NPP_GIPT_CSV}")
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    npp_plants = plants_df[plants_df["source_system"] == "NPP"]
    if npp_plants.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    logger.info(
        f"NPP-GIPT authoritative matching for {len(npp_plants):,} NPP plants..."
    )

    gipt = pd.read_csv(NPP_GIPT_CSV)
    gipt_coal = gipt[gipt["Type"].astype(str).str.lower() == "coal"].copy()
    logger.info(
        f"  GIPT crosswalk: {len(gipt_coal):,} coal unit rows across "
        f"{gipt_coal['DGR plant name'].nunique():,} distinct DGR plants"
    )

    if not GEM_CSV.exists():
        logger.warning(f"GEM CSV not found: {GEM_CSV}")
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    gem_raw = pd.read_csv(GEM_CSV, low_memory=False)
    gem_by_uid = gem_raw.set_index("Unit ID", drop=False)

    results = []
    npp_names = set(npp_plants["plant_name"].dropna().astype(str).unique())
    # Map normalized key -> actual name as stored in npp_generation, so coal
    # classification survives whitespace/case differences in the crosswalk and
    # the stored `plant_name` still equals the value the dashboard joins on.
    # Build from a sorted list so a (rare) normalization collision resolves
    # deterministically rather than by set-iteration order.
    npp_by_norm = {_norm_npp_name(n): n for n in sorted(npp_names)}

    for dgr_name, group in gipt_coal.groupby("DGR plant name"):
        matched_npp_name = npp_by_norm.get(_norm_npp_name(dgr_name))
        if matched_npp_name is None:
            continue

        unit_caps = []
        coal_types = []
        techs = []
        lats, lons = [], []
        gem_project_names = []
        for _, row in group.iterrows():
            uid = row.get("GEM unit/phase ID")
            if not isinstance(uid, str) or uid not in gem_by_uid.index:
                continue
            gem_row = gem_by_uid.loc[uid]
            if isinstance(gem_row, pd.DataFrame):
                gem_row = gem_row.iloc[0]
            cap = _parse_gem_capacity(gem_row.get("Capacity"))
            if cap is not None:
                unit_caps.append(cap)
            ct = _parse_gem_coal_type(gem_row.get("Fuel"))
            if ct:
                coal_types.append(ct)
            tech = _normalize_combustion_tech(gem_row.get("Technology"))
            if tech:
                techs.append(tech)
            lat, lon = gem_row.get("Latitude"), gem_row.get("Longitude")
            if pd.notna(lat) and pd.notna(lon):
                lats.append(float(lat))
                lons.append(float(lon))
            pn = gem_row.get("Project Name")
            if isinstance(pn, str):
                gem_project_names.append(pn)

        # Use first valid coords; sum unit capacities; first coal_type / tech as representative.
        plant_cap = sum(unit_caps) if unit_caps else None
        plant_lat = lats[0] if lats else None
        plant_lon = lons[0] if lons else None
        # These plants are authoritatively coal (GIPT Type == "coal"); GEM often
        # lacks a sub-type ("coal" with no qualifier, multi-fuel, or "unknown"),
        # which would leave coal_type NULL and make the dashboard — which keys
        # coal classification off coal_type IS NOT NULL — drop them. Default to
        # "unknown" (a value the dashboard already handles) so every coal plant
        # is classified as coal.
        plant_coal = coal_types[0] if coal_types else "unknown"
        plant_tech = techs[0] if techs else None
        plant_state = (
            group["State"].dropna().iloc[0] if group["State"].notna().any() else None
        )
        plant_sector = (
            group["Sector"].dropna().iloc[0] if group["Sector"].notna().any() else None
        )
        ref_name = gem_project_names[0] if gem_project_names else None

        if plant_lat is None or plant_lon is None:
            # Authoritative crosswalk match but GEM has no coordinates — skip;
            # rapidfuzz/LLM may still find a different reference.
            continue

        results.append(
            {
                "plant_name": matched_npp_name,
                "plant_code": None,
                "source_system": "NPP",
                "latitude": plant_lat,
                "longitude": plant_lon,
                "ref_source": "GEM",
                "matching_method": "direct",
                "confidence": "high",
                "ref_matched_name": ref_name,
                "coal_type": plant_coal,
                "combustion_tech": plant_tech,
                "capacity_mw": plant_cap,
                "state": plant_state,
                "sector": plant_sector,
            }
        )

    out = (
        pd.DataFrame(results, columns=OUTPUT_COLUMNS)
        if results
        else pd.DataFrame(columns=OUTPUT_COLUMNS)
    )
    # Distinct crosswalk `DGR plant name` entries can collapse to the same NPP
    # plant under normalization (e.g. ' OPG...' vs 'OPG...'). Keep one row per
    # plant_name so the dashboard's plant-level LEFT JOIN doesn't double-count.
    if not out.empty:
        out = out.drop_duplicates(subset=["plant_name"], keep="first").reset_index(
            drop=True
        )
    logger.info(
        f"  NPP-GIPT direct: {len(out):,} matched (with capacity, state, sector)"
    )
    return out


def match_direct(plants_df: pd.DataFrame) -> pd.DataFrame:
    """Direct matching for OE plants that already have embedded coordinates."""
    results = []

    oe_plants = plants_df[plants_df["source_system"] == "OE"].copy()
    if not oe_plants.empty:
        logger.info("OE direct coordinate matching...")
        for _, row in oe_plants.iterrows():
            lat, lon = row.get("latitude"), row.get("longitude")
            if pd.notna(lat) and pd.notna(lon) and validate_coordinates(lat, lon):
                results.append(
                    {
                        "plant_name": row["plant_name"],
                        "plant_code": row.get("plant_code"),
                        "source_system": "OE",
                        "latitude": lat,
                        "longitude": lon,
                        "ref_source": "OE-direct",
                        "matching_method": "direct",
                        "confidence": None,
                        "ref_matched_name": row["plant_name"],
                    }
                )
        logger.info(f"  OE direct: {len(results):,} matched")

    return (
        pd.DataFrame(results, columns=OUTPUT_COLUMNS)
        if results
        else pd.DataFrame(columns=OUTPUT_COLUMNS)
    )


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
        gem_norm = build_norm_index(
            gem_names, normalize_for_comparison, f"GEM[{source}]"
        )
        gem_norm_list = list(gem_norm.keys())

        # Load GPPD for this source's countries
        cfg = SOURCE_COUNTRIES.get(source, {})
        gppd_countries = cfg.get("gppd_countries") or (
            [cfg["gppd"]] if cfg.get("gppd") else None
        )
        gppd_df = load_gppd(gppd_countries)
        gppd_raw_names = gppd_df["name"].dropna().unique().tolist()
        gppd_norm = build_norm_index(
            gppd_raw_names, normalize_gppd_name, f"GPPD[{source}]"
        )
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
            if not norm_name:
                # Names that normalize to empty (e.g. "POWER PLANT (Liq.)")
                # must not be fuzzy-matched — empty-vs-anything is garbage.
                continue
            matched = False

            # --- GEM: token_sort_ratio ---
            if gem_norm_list and (ref_sources is None or "GEM" in ref_sources):
                gem_hit = process.extractOne(
                    norm_name,
                    gem_norm_list,
                    scorer=fuzz.token_sort_ratio,
                    score_cutoff=GEM_THRESHOLD,
                )
                # validate_match guards only the marginal score band: the
                # threshold-80 false positives the code documents ("BHADRA
                # HPS" → "Bhandara power station") score in the 80s, while
                # true transliteration variants score ≥ VALIDATE_TRUST_SCORE
                # and pass unguarded. Rejected hits fall through to the LLM
                # stage, which is much better at telling such pairs apart.
                if (
                    gem_hit
                    and gem_hit[1] < VALIDATE_TRUST_SCORE
                    and not validate_match(plant_name, gem_norm[gem_hit[0]])
                ):
                    logger.debug(
                        f"{source}: marginal fuzzy GEM hit (score {gem_hit[1]:.0f}) "
                        f"rejected by validate_match: "
                        f"{plant_name!r} → {gem_norm[gem_hit[0]]!r}"
                    )
                    gem_hit = None
                if gem_hit:
                    orig = gem_norm[gem_hit[0]]
                    info = gem_names[orig]
                    if validate_coordinates(info["lat"], info["lon"]):
                        results.append(
                            {
                                "plant_name": plant_name,
                                "plant_code": row.get("plant_code"),
                                "source_system": source,
                                "latitude": info["lat"],
                                "longitude": info["lon"],
                                "ref_source": "GEM",
                                "matching_method": "rapidfuzz",
                                "confidence": None,
                                "ref_matched_name": orig,
                                # Suppress coal-metadata attribution when an NPP plant's
                                # name has a non-coal technology suffix (HPS/CCPP/etc.).
                                # This avoids spurious "coal" capacity on hydro/gas
                                # plants that fuzzy-matched to a similarly-named coal plant.
                                "coal_type": None
                                if (
                                    source == "NPP"
                                    and _is_npp_likely_non_coal(plant_name)
                                )
                                else info.get("coal_type"),
                                "combustion_tech": None
                                if (
                                    source == "NPP"
                                    and _is_npp_likely_non_coal(plant_name)
                                )
                                else info.get("combustion_tech"),
                                "capacity_mw": None
                                if (
                                    source == "NPP"
                                    and _is_npp_likely_non_coal(plant_name)
                                )
                                else info.get("capacity_mw"),
                            }
                        )
                        matched = True
                        count += 1

            # --- GPPD: token_sort_ratio ---
            if (
                not matched
                and gppd_norm_list
                and (ref_sources is None or "GPPD" in ref_sources)
            ):
                gppd_query = normalize_gppd_name(plant_name)
                gppd_hit = (
                    process.extractOne(
                        gppd_query,
                        gppd_norm_list,
                        scorer=fuzz.token_sort_ratio,
                        score_cutoff=GPPD_THRESHOLD,
                    )
                    if gppd_query
                    else None
                )
                if (
                    gppd_hit
                    and gppd_hit[1] < VALIDATE_TRUST_SCORE
                    and not validate_match(plant_name, gppd_norm[gppd_hit[0]])
                ):
                    logger.debug(
                        f"{source}: marginal fuzzy GPPD hit (score {gppd_hit[1]:.0f}) "
                        f"rejected by validate_match: "
                        f"{plant_name!r} → {gppd_norm[gppd_hit[0]]!r}"
                    )
                    gppd_hit = None
                if gppd_hit:
                    orig = gppd_norm[gppd_hit[0]]
                    coords = gppd_coords.get(orig, {})
                    if validate_coordinates(coords.get("lat"), coords.get("lon")):
                        results.append(
                            {
                                "plant_name": plant_name,
                                "plant_code": row.get("plant_code"),
                                "source_system": source,
                                "latitude": coords["lat"],
                                "longitude": coords["lon"],
                                "ref_source": "GPPD",
                                "matching_method": "rapidfuzz",
                                "confidence": None,
                                "ref_matched_name": orig,
                            }
                        )
                        matched = True
                        count += 1

        logger.info(f"  {source} rapidfuzz: {count:,} matched")

    return (
        pd.DataFrame(results, columns=OUTPUT_COLUMNS)
        if results
        else pd.DataFrame(columns=OUTPUT_COLUMNS)
    )


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

    for source in unmatched["source_system"].unique():
        src_plants = unmatched[unmatched["source_system"] == source]
        if src_plants.empty:
            continue

        logger.info(f"LLM matching {len(src_plants):,} {source} plants...")

        # Build reference lists for candidate retrieval
        gem_names = load_gem(source)
        gem_name_list = list(gem_names.keys())

        cfg = SOURCE_COUNTRIES.get(source, {})
        gppd_countries = cfg.get("gppd_countries") or (
            [cfg["gppd"]] if cfg.get("gppd") else None
        )
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
            "GEM": {
                n: {
                    "lat": info["lat"],
                    "lon": info["lon"],
                    "coal_type": info.get("coal_type"),
                    "combustion_tech": info.get("combustion_tech"),
                    "capacity_mw": info.get("capacity_mw"),
                }
                for n, info in gem_names.items()
            },
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

            confidence = _normalize_confidence(result.confidence)
            # isinstance guard: result.match is parsed.get("match") — whatever
            # JSON type the LLM returned. A truthy non-string (dict/list/number
            # from a malformed response) would reach _clean_llm_match's
            # .strip() and raise AttributeError, propagating out of match_llm
            # and discarding every match accumulated in a paid run. Treat a
            # non-string match as no-match: the plant falls through to unmatched.
            if (
                isinstance(result.match, str)
                and result.match
                and confidence in ("high", "medium")
            ):
                # The "SOURCE: " prefix in the match text is authoritative —
                # the model's separate `source` field sometimes answers
                # "Crosswalk" (an option the prompt offers but all_coords
                # doesn't carry) or varies in case, which used to drop
                # structurally valid matches with no log.
                prefix_source, matched_name = _clean_llm_match(result.match)
                ref_source = prefix_source or result.source

                # Look up coordinates
                coords = all_coords.get(ref_source, {}).get(matched_name, {})
                if not coords:
                    # Last resort: search the other reference sets by name —
                    # but only accept an UNAMBIGUOUS hit. The same plant name
                    # can exist in both GEM and GPPD with different
                    # coordinates; guessing (first source wins) would be a
                    # silent wrong-coordinate path.
                    holders = [
                        (cand_source, cand_coords[matched_name])
                        for cand_source, cand_coords in all_coords.items()
                        if matched_name in cand_coords
                    ]
                    if len(holders) == 1:
                        ref_source, coords = holders[0]
                    elif len(holders) > 1:
                        logger.warning(
                            f"{source}: LLM match {matched_name!r} for "
                            f"{plant_name!r} is ambiguous across "
                            f"{[s for s, _ in holders]} — discarded"
                        )
                lat, lon = coords.get("lat"), coords.get("lon")

                coords_ok = validate_coordinates(lat, lon)
                if not coords_ok:
                    logger.warning(
                        f"{source}: LLM match for {plant_name!r} DISCARDED — "
                        f"could not resolve {ref_source!r}/{matched_name!r} to "
                        f"valid coordinates (llm source field: {result.source!r})"
                    )
                if coords_ok:
                    results.append(
                        {
                            "plant_name": plant_name,
                            "plant_code": row.get("plant_code"),
                            "source_system": source,
                            "latitude": lat,
                            "longitude": lon,
                            "ref_source": ref_source or "LLM",
                            "matching_method": "llm",
                            "confidence": confidence,
                            "ref_matched_name": matched_name,
                            "reasoning": result.reasoning,
                            # Same NPP non-coal suppression as the rapidfuzz path.
                            "coal_type": None
                            if (source == "NPP" and _is_npp_likely_non_coal(plant_name))
                            else coords.get("coal_type"),
                            "combustion_tech": None
                            if (source == "NPP" and _is_npp_likely_non_coal(plant_name))
                            else coords.get("combustion_tech"),
                            "capacity_mw": None
                            if (source == "NPP" and _is_npp_likely_non_coal(plant_name))
                            else coords.get("capacity_mw"),
                        }
                    )

        logger.info(
            f"  {source} LLM: {len([r for r in results if r['source_system'] == source]):,} matched"
        )

    return (
        pd.DataFrame(results, columns=OUTPUT_COLUMNS)
        if results
        else pd.DataFrame(columns=OUTPUT_COLUMNS)
    )


def _log_per_source(matched_df: pd.DataFrame, input_df: pd.DataFrame, stage: str):
    """Log per-source breakdown for a matching stage."""
    for src in input_df["source_system"].unique():
        src_total = len(input_df[input_df["source_system"] == src])
        src_matched = (
            len(matched_df[matched_df["source_system"] == src])
            if not matched_df.empty
            else 0
        )
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
        logger.info(
            f"  Kept {len(existing):,} rows (excluded {', '.join(sources)} for rebuild)"
        )
    elif not sources and OUTPUT_FILE.exists():
        logger.info(f"Found existing output: {OUTPUT_FILE}")
        cached = pd.read_parquet(OUTPUT_FILE)
        logger.info(
            f"  {len(cached):,} rows, {cached['latitude'].notna().mean():.1%} with coords"
        )
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
    plants_df = plants_df.drop_duplicates(
        subset=["plant_name", "source_system"], keep="first"
    )
    logger.info(
        f"After dedup: {len(plants_df):,} unique (plant_name, source_system) pairs"
    )

    # Step 3a: Direct matching (OE embedded coords + NPP via GIPT crosswalk)
    logger.info("=" * 60)
    logger.info("Step 3: Direct matching (OE embedded coords + NPP-GIPT)...")
    exact_oe = match_direct(plants_df)
    exact_npp = match_npp_via_gipt(plants_df)
    exact_df = (
        pd.concat([exact_oe, exact_npp], ignore_index=True)
        if not exact_npp.empty
        else exact_oe
    )
    logger.info(
        f"Direct matches: OE={len(exact_oe):,} + NPP-GIPT={len(exact_npp):,} = {len(exact_df):,}"
    )

    # Determine unmatched
    matched_keys = (
        set(zip(exact_df["plant_name"], exact_df["source_system"]))
        if not exact_df.empty
        else set()
    )
    unmatched_mask = ~plants_df.apply(
        lambda r: (r["plant_name"], r["source_system"]) in matched_keys, axis=1
    )
    unmatched_1 = plants_df[unmatched_mask]
    logger.info(f"Unmatched after exact: {len(unmatched_1):,}")

    # Step 4: Rapidfuzz matching (GEM)
    logger.info("=" * 60)
    logger.info("Step 4: Rapidfuzz matching (GEM)...")
    gem_df = match_rapidfuzz(unmatched_1, ref_sources=["GEM"])
    logger.info(f"GEM matches: {len(gem_df):,}")
    _log_per_source(gem_df, unmatched_1, "GEM rapidfuzz")

    # Update unmatched after GEM
    gem_keys = (
        set(zip(gem_df["plant_name"], gem_df["source_system"]))
        if not gem_df.empty
        else set()
    )
    all_matched_gem = matched_keys | gem_keys
    unmatched_after_gem = plants_df[
        ~plants_df.apply(
            lambda r: (r["plant_name"], r["source_system"]) in all_matched_gem, axis=1
        )
    ]
    logger.info(f"Unmatched after GEM: {len(unmatched_after_gem):,}")

    # Step 5: Rapidfuzz matching (GPPD)
    logger.info("=" * 60)
    logger.info("Step 5: Rapidfuzz matching (GPPD)...")
    gppd_df = match_rapidfuzz(unmatched_after_gem, ref_sources=["GPPD"])
    logger.info(f"GPPD matches: {len(gppd_df):,}")
    _log_per_source(gppd_df, unmatched_after_gem, "GPPD rapidfuzz")

    # Update unmatched after GPPD
    gppd_keys = (
        set(zip(gppd_df["plant_name"], gppd_df["source_system"]))
        if not gppd_df.empty
        else set()
    )
    all_matched = all_matched_gem | gppd_keys
    unmatched_2 = plants_df[
        ~plants_df.apply(
            lambda r: (r["plant_name"], r["source_system"]) in all_matched, axis=1
        )
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
            confirm = input(
                f"Proceed with LLM matching for {n_plants:,} plants (~${est_cost:.2f})? [y/N] "
            )
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
    llm_keys = (
        set(zip(llm_df["plant_name"], llm_df["source_system"]))
        if not llm_df.empty
        else set()
    )
    final_matched = all_matched | llm_keys
    still_unmatched = plants_df[
        ~plants_df.apply(
            lambda r: (r["plant_name"], r["source_system"]) in final_matched, axis=1
        )
    ]

    unmatched_rows = []
    for _, row in still_unmatched.iterrows():
        unmatched_rows.append(
            {
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
                "coal_type": None,
                "combustion_tech": None,
                "capacity_mw": None,
            }
        )
    unmatched_df = pd.DataFrame(unmatched_rows, columns=OUTPUT_COLUMNS)

    new_rows = pd.concat(
        [exact_df, gem_df, gppd_df, llm_df, unmatched_df], ignore_index=True
    )

    # Expand EIA rows: if multiple plant_codes share the same plant_name,
    # create one crosswalk row per plant_code (all sharing the same coords)
    eia_rows = new_rows[new_rows["source_system"] == "EIA"]
    non_eia_rows = new_rows[new_rows["source_system"] != "EIA"]
    if not eia_rows.empty and not eia_code_map.empty:
        # Drop the single plant_code from matching, re-join with full mapping
        eia_expanded = eia_rows.drop(columns=["plant_code"]).merge(
            eia_code_map,
            on="plant_name",
            how="left",
        )
        new_rows = pd.concat([non_eia_rows, eia_expanded], ignore_index=True)
        n_added = len(new_rows) - len(non_eia_rows) - len(eia_rows)
        if n_added > 0:
            logger.info(
                f"Expanded {n_added} additional EIA rows for duplicate plant names"
            )

    # Merge with existing crosswalk when running for specific sources
    if existing is not None:
        unified = pd.concat([existing, new_rows], ignore_index=True)
        logger.info(
            f"Merged {len(new_rows):,} new rows with {len(existing):,} existing → {len(unified):,} total"
        )
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
    logger.info(
        f"  With coords:     {unified['latitude'].notna().sum():,} ({coverage:.1%})"
    )
    logger.info(
        f"  Without coords:  {unified['latitude'].isna().sum():,} ({1 - coverage:.1%})"
    )
    logger.info("\n  By source_system:")
    for src in unified["source_system"].unique():
        subset = unified[unified["source_system"] == src]
        n = len(subset)
        cov = subset["latitude"].notna().mean()
        logger.info(f"    {src:8s}: {n:6,} plants, {cov:.1%} coverage")
    logger.info("\n  By matching_method:")
    for method, count in unified["matching_method"].value_counts(dropna=False).items():
        label = method if pd.notna(method) else "unmatched"
        logger.info(f"    {label:12s}: {count:,}")

    return unified


def main():
    import argparse

    valid_sources = list(SOURCE_COUNTRIES.keys())

    parser = argparse.ArgumentParser(
        description="Build unified plant coordinate crosswalk"
    )
    parser.add_argument("--no-llm", action="store_true", help="Skip LLM matching step")
    parser.add_argument(
        "--force", action="store_true", help="Overwrite existing output file"
    )
    parser.add_argument(
        "--sources",
        nargs="+",
        choices=valid_sources,
        metavar="SOURCE",
        help=f"Only process specific sources (appends to existing). Choices: {', '.join(valid_sources)}",
    )
    parser.add_argument(
        "--yes", "-y", action="store_true", help="Skip interactive confirmations"
    )
    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="INFO")

    if args.force and not args.sources and OUTPUT_FILE.exists():
        OUTPUT_FILE.unlink()
        logger.info(f"Removed existing output: {OUTPUT_FILE}")

    build_unified_crosswalk(skip_llm=args.no_llm, sources=args.sources, yes=args.yes)


if __name__ == "__main__":
    main()

"""Shared plant name normalization utilities.

These functions are used by both the NPP coverage notebook and the
unified crosswalk builder script. Originally lived in the notebook
and in dashboard/src/plant_coordinates.py.
"""

import re

import pandas as pd

_STOP_WORDS = {
    "power",
    "plant",
    "energy",
    "thermal",
    "station",
    "project",
    "limited",
    "ltd",
    "pvt",
    "private",
    "corporation",
    "corp",
    "company",
    "group",
    "industries",
    "generation",
    "electric",
}

# Suffixes found in NPP names (uppercase) — ordered longest first
_NPP_SUFFIXES = [
    " REPLACEMENT POWER PROJECT",
    " SUPER THERMAL POWER STATION",
    " STPS",
    " STPP",
    " TPS",
    " TPP",
    " UMTPP",
    " SCTPP",
    " NCTPP",
    " CCPP",
    " CCGT",
    " HPS",
    " HEP",
    " A.P.S.",
    " PSP",
    " PSS",
    " GPS",
    " GT",
    " DG",
    " D.G",
    " ST-I",
    " ST-II",
    " ST-III",
    " ST-IV",
    " ST-1",
    " ST-2",
    " ST-3",
    " ST-4",
    " PH-I",
    " PH-II",
    " EXTN",
    " EXT",
    " EXP",
    " IMP",
    " PH I",
    " PH II",
    " (Liq.)",
    " (NCTPP)",
]

# Suffixes found in GEM names (title/mixed case)
_GEM_SUFFIXES = [
    " power station",
    " power plant",
    " thermal power plant",
    " thermal power station",
    " super thermal power station",
    " Super Thermal Power Station",
    " Thermal Power Plant",
    " Power Station",
    " Power Plant",
    " project",
    " Project",
]


def _strip_suffixes_anchored(name: str, suffixes) -> str:
    """Repeatedly strip any of the given suffixes from the END of the name.

    Anchored on purpose: the old `str.replace` deleted these fragments
    ANYWHERE in the string, mangling real location names (" EXT" turned
    "WEST EXTENSION" into "WESTENSION", " IMP" turned "PUNTA IMPERIAL"
    into "PUNTA ERIAL") — and the mangled forms were the fuzzy-matching
    keys on BOTH sides, creating false-positive matches between distinct
    plants. Iterates so stacked suffixes ("FOO TPS EXT") fully strip.
    """
    result = name.rstrip()
    changed = True
    while changed:
        changed = False
        for s in suffixes:
            if result.endswith(s):
                result = result[: -len(s)].rstrip()
                changed = True
    return result


def extract_base_name(name: str) -> str:
    """Strip common Indian power-plant suffixes to get the core location name."""
    return _strip_suffixes_anchored(name, _NPP_SUFFIXES).strip()


def normalize_for_comparison(name: str) -> str:
    """Normalize a plant name for comparison: uppercase, strip all known suffixes, clean."""
    n = name.upper().strip()
    # Parentheticals first: a trailing "(LIQ.)" would otherwise block the
    # end-anchored suffix strip (e.g. "BARAUNI TPS (Liq.)" must reach
    # "BARAUNI TPS" before " TPS" can strip).
    n = re.sub(r"\([^)]*\)", "", n).strip()
    n = _strip_suffixes_anchored(n, _NPP_SUFFIXES)
    n = _strip_suffixes_anchored(n, [s.upper() for s in _GEM_SUFFIXES])
    n = re.sub(r"[^A-Z0-9\s]", " ", n)
    n = " ".join(n.split())
    return n


def validate_match(plant_name: str, matched_name: str, min_word_len: int = 4) -> bool:
    """Check that at least one significant location word from the query appears in the match."""
    base = extract_base_name(plant_name).lower()
    base_words = base.replace("-", " ").split()
    match_lower = matched_name.lower()
    return any(
        re.search(r"\b" + re.escape(w) + r"\b", match_lower)
        for w in base_words
        if len(w) >= min_word_len and w not in _STOP_WORDS
    )


def normalize_gppd_name(name: str) -> str:
    """Normalize plant name for GPPD matching."""
    if pd.isna(name):
        return ""
    name = str(name).upper()
    name = _strip_suffixes_anchored(name, _NPP_SUFFIXES)
    name = re.sub(
        r"\s+(POWER\s+PLANT|POWER\s+STATION|THERMAL|TPS|CCPP|CCGT|HPS|GT|DG|BLOCK\s*[A-Z0-9]*)\s*$",
        "",
        name,
    )
    name = re.sub(r"[^A-Z0-9\s]", " ", name)
    name = " ".join(name.split())
    return name

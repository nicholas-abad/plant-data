"""The Europe rows of unit_gem_map: each ENTSO-E coal unit's GEM unit.

ENTSO-E reports generation per unit, and each unit name is its own row in the
crosswalk ("Neurath G", "Kozienice 2 B11", "TE Kostolac A G1",
"CTE_Rovinari_ROVI5_CA"). GEM's ENTSO-E codes are almost all gas units, so
within the GEM plant the crosswalk links the name to, a name matches the coal
unit GEM numbers or letters the same ("Unit G", "Phase A Unit 1", "Unit I-3"),
or the only unit when both sides have one. A GEM unit two names claim while
both generate in the same month waits for review (a crosswalk link putting
two plants on one site); successive spellings of one unit, which never
overlap, share it. Cases the names do not settle (Bełchatów B14 = Phase 2 Unit
1, Eemshaven A/B = Units 1/2) come from the reviewed file
data/crosswalks/entsoe_unit_gem_map.csv. Everything else sits at its plant's
crosswalk link (fallback rows).
"""

from __future__ import annotations

import re
import unicodedata

import pandas as pd
from loguru import logger
from sqlalchemy import text

from .eia_unit_map import NEVER_RAN
from .unit_gem_map import COLUMNS, check_reviewed, with_fallback
from .utils import get_crosswalk_dir

REVIEWED_FILE = get_crosswalk_dir() / "entsoe_unit_gem_map.csv"
COAL_FUELS = ("Fossil Hard coal", "Fossil Brown coal/Lignite")
BY_NUMBER = "unit-number"
SINGLE = "single-unit"
REVIEWED = "source-id-reviewed"

ROMAN = {"I": "1", "II": "2", "III": "3", "IV": "4", "V": "5", "VI": "6", "VII": "7", "VIII": "8", "IX": "9", "X": "10"}
_ROMAN_RE = "I{1,3}|IV|VI{0,3}|IX|X"


def _norm(token: str) -> str:
    token = token.upper()
    return ROMAN.get(token) or token.lstrip("0") or "0"


def entsoe_keys(name) -> list[tuple[str | None, str]]:
    """(phase, unit) readings of an ENTSO-E unit name, most specific first.

    'TE Kostolac A G1' → [('A', '1'), (None, '1')]; 'Neurath G' → [(None, 'G')];
    'PTOLEMAIDA5' → [(None, '5')]; 'AGIOS DIMITRIOS V' → [(None, 'V'), (None, '5')].
    """
    s = unicodedata.normalize("NFKC", str(name or ""))
    s = re.sub(r"\(.*?\)", " ", s)
    s = re.sub(r"_+", " ", re.sub(r"_CA$", "", s.strip(" _-."))).strip()
    keys = []
    m = re.search(rf"(?:^|\s)([A-Z]|\d|{_ROMAN_RE})\s+(?:G|B|TG|BL|BLOCK)?\s*(\d+)$", s, re.I)
    if m:
        keys.append((_norm(m.group(1)), _norm(m.group(2))))
    m = re.search(rf"(?:^|[\s.])(?:BLOCK|BLOK|UNIT|GR|TG|EB|BL|B|G|U|K)?\s*(\d+|[A-Z]|{_ROMAN_RE})$", s, re.I)
    if m:
        t = m.group(1).upper()
        if t.isdigit() or len(t) == 1:
            keys.append((None, t.lstrip("0") or "0"))
        if t in ROMAN:
            keys.append((None, ROMAN[t]))
    # digits written onto a word: PTOLEMAIDA5, ROVI5, KS_TGB1, MÁ2_gép4 (not plant codes like TPP_MI2)
    m = re.search(r"(?:^|\s)(?:TG[A-Z]?|gép|[^\W\d_]{4,})(\d+)$", s, re.I)
    if m:
        keys.append((None, _norm(m.group(1))))
    return list(dict.fromkeys(keys))


def gem_key(unit_name) -> tuple[str | None, str | None]:
    """GEM's 'Phase A Unit 1' / 'Unit I-3' / 'Unit 5, timepoint 1' / 'Phase B' → (phase, unit)."""
    s = re.sub(r",\s*timepoint\s*\d+", "", str(unit_name or ""))
    s = re.sub(r"\(.*?\)", " ", s).strip()
    m = re.search(rf"Unit\s+({_ROMAN_RE})-(\d+)$", s)
    if m:
        return _norm(m.group(1)), _norm(m.group(2))
    ph = re.search(r"\bPhase\s+(\w+)\b", s)
    phase = _norm(ph.group(1)) if ph else None
    m = re.search(r"(?:Unit|CHP|Block)\s*[- ]?(?:TG)?(\d+|[A-Z])$", s) or re.search(r"[- ](\d+)$", s)
    if m:
        return phase, _norm(m.group(1))
    if ph and re.fullmatch(r"Phase\s+\w+", s):
        return None, phase
    return phase, None


def pull_months(engine) -> pd.DataFrame:
    """Months each coal unit name generated in (plant_name, month)."""
    with engine.connect() as conn:
        return pd.read_sql(
            text(
                "SELECT DISTINCT plant_name, CASE WHEN generation_mwh > 0 THEN month END AS month "
                "FROM mv_entsoe_plant_monthly WHERE fuel_type = ANY(:coal) AND plant_name IS NOT NULL"
            ),
            conn,
            params={"coal": list(COAL_FUELS)},
        )


def build_entsoe_unit_map(
    months: pd.DataFrame, reviewed: pd.DataFrame, gem_units: pd.DataFrame, plant_links: dict[str, str]
) -> pd.DataFrame:
    """``months``: one row per (plant_name, month generated); a NULL month lists a name that never did."""
    coal_gem = gem_units[
        (gem_units["tracker"] == "GCPT") & ~gem_units["status"].astype(str).str.lower().isin(NEVER_RAN)
    ]
    keys = coal_gem["unit_name"].map(gem_key)
    coal_gem = coal_gem.assign(phase=keys.str[0], unit=keys.str[1])
    by_loc = {loc: grp for loc, grp in coal_gem.groupby("gem_location_id")}
    review = reviewed.set_index("plant_name")
    names = months["plant_name"].drop_duplicates()
    names_per_loc = names.map(plant_links).value_counts().to_dict()
    rows = []
    for name in names:
        if name in review.index:
            hit = review.loc[name]
            unit_ids, loc, how = hit["gem_unit_ids"], hit["gem_location_id"], REVIEWED
        else:
            loc = plant_links.get(name)
            grp = by_loc.get(loc)
            if grp is None:
                continue
            unit_ids = None
            readings = entsoe_keys(name)
            # a phase in the name rules out GEM units of another phase
            named_phase = next((phase for phase, _ in readings if phase), None)
            if named_phase:
                grp = grp[grp["phase"].isna() | (grp["phase"] == named_phase)]
            for phase, unit in readings:
                same = grp[(grp["unit"] == unit) & ((grp["phase"] == phase) if phase else True)]
                if len(same) == 1:
                    unit_ids, how = same["gem_unit_id"].iloc[0], BY_NUMBER
                    break
            if unit_ids is None and len(grp) == len(by_loc[loc]) == 1 and names_per_loc.get(loc) == 1:
                unit_ids, how = grp["gem_unit_id"].iloc[0], SINGLE
            if unit_ids is None:
                continue
        rows.append(
            {
                "source_system": "ENTSOE",
                "plant_key": name,
                "unit_key": "",
                "gem_location_id": loc,
                "gem_unit_id": unit_ids if len(unit_ids.split()) == 1 else None,
                "gem_unit_ids": unit_ids,
                "map_source": how,
                "fuel": "coal",
                "is_coal": True,
                "other_gem_unit_ids": None,
            }
        )
    matched = pd.DataFrame(rows, columns=COLUMNS)
    # Two names on one GEM unit generating in the same month are two units
    # (TPP_MI2_G3 and TPP_MI3_G3 both linked to Maritsa Iztok-2): review.
    auto = matched[matched["map_source"] != REVIEWED][["plant_key", "gem_unit_ids"]]
    gen = months.dropna(subset=["month"]).merge(auto, left_on="plant_name", right_on="plant_key")
    per_month = gen.groupby(["gem_unit_ids", "month"])["plant_key"].nunique()
    clash = set(per_month[per_month > 1].index.get_level_values(0))
    matched = matched[~(matched["gem_unit_ids"].isin(clash) & (matched["map_source"] != REVIEWED))]
    every = pd.DataFrame({"source_system": "ENTSOE", "plant_key": names, "unit_key": "", "fuel": "coal", "is_coal": True})
    out = with_fallback(matched, every, plant_links)
    logger.info(
        f"unit_gem_map ENTSOE: {len(matched)}/{len(out)} linked coal names on a GEM unit "
        f"({(matched['map_source'] == BY_NUMBER).sum()} by number, {(matched['map_source'] == SINGLE).sum()} single, "
        f"{(matched['map_source'] == REVIEWED).sum()} reviewed; {len(clash)} GEM units left for review as claimed "
        f"twice in one month); {len(out) - len(matched):,} fallback rows; "
        f"{len(names) - len(out)} coal names have no GEM plant link"
    )
    return out


def build_from_db(engine, gem_units: pd.DataFrame, crosswalk: pd.DataFrame) -> pd.DataFrame:
    reviewed = pd.read_csv(REVIEWED_FILE, dtype=str, keep_default_na=False)
    check_reviewed(REVIEWED_FILE, reviewed, gem_units, ["plant_name"])
    entsoe = crosswalk[(crosswalk["source_system"] == "ENTSOE") & crosswalk["gem_location_id"].notna()]
    return build_entsoe_unit_map(
        pull_months(engine), reviewed, gem_units, dict(zip(entsoe["plant_name"], entsoe["gem_location_id"]))
    )

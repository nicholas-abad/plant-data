"""Link EIA plants to GEM locations by the EIA plant code GEM records.

Name similarity put 58 EIA plants on the wrong GEM location (Coal Creek, ND
on Coleto Creek, TX; Prairie State, IL on W A Parish, TX; …), about a tenth of
US coal every year since 2019. GEM lists each US location's EIA plant code
(``gem_external_ids``, namespace ``EIA``), so the crosswalk links by that code
first and leaves name matching for plants GEM does not code.

A code links automatically only when GEM gives it exactly one location, that
location is in the gem_locations mirror, and its state equals the state EIA
reports for the plant. GEM's codes are not perfect (EIA 10307 Bellingham, MA
sits at a location GEM names "Toronto Pearson Airport cogeneration complex"),
so every link this changes or adds is listed in the reviewed file
``data/crosswalks/eia_gem_id_review.csv``:

* ``accept``  apply the file's gem_location_id (also overrides a failed rule);
* ``reject``  GEM's code is wrong for this plant: never apply it;
* ``defer``   not decided yet: never apply it.

A rejected or deferred code goes through the ordinary name matching instead.
Codes not in the file follow the automatic rule, so plants EIA adds later
link without a review round.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from loguru import logger

from . import gem_reference as gemref
from .utils import get_crosswalk_dir

REVIEW_FILE = get_crosswalk_dir() / "eia_gem_id_review.csv"
# Reviewed old → new decision changes (import_decisions.py --replace-file);
# verify_crosswalk.py's gate6f accepts exactly these changes.
MIGRATIONS_DIR = get_crosswalk_dir() / "decision_migrations"
# Pipeline-made "decisions" tier 0 re-emits; a GEM code may supersede them.
# Anything else in decided_by is a person's decision.
PIPELINE_DECIDERS = {"legacy-pipeline", "gppd-migration-2026-09"}
METHOD = "gem-eia-id"
VERDICTS = {"accept", "reject", "defer"}
REVIEW_COLUMNS = [
    "plant_code",
    "plant_name",
    "eia_state",
    "current_gem_location_id",
    "current_name",
    "current_method",
    "current_decided_by",
    "gem_location_id",
    "gem_name",
    "gem_state",
    "reason",
    "verdict",
    "note",
    "reviewed_by",
    "reviewed_on",
]

US_STATES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "DC": "District of Columbia", "FL": "Florida", "GA": "Georgia", "HI": "Hawaii",
    "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming", "PR": "Puerto Rico",
}  # fmt: skip


@dataclass(frozen=True)
class Target:
    """Where GEM's code puts an EIA plant, or why it links nowhere."""

    gem_location_id: str | None
    reason: str  # 'rule', 'review:accept', or why not


def eia_state_name(states) -> str | None:
    """EIA's state code(s) for a plant ('TX', or 'TX|OK' if it reports several) → GEM's name; None unless exactly one."""
    if not isinstance(states, str) or not states.strip():
        return None
    codes = {s.strip() for s in states.split("|") if s.strip()}
    return US_STATES.get(codes.pop()) if len(codes) == 1 else None


def gem_state(loc_id: str) -> str | None:
    locs = gemref.load_tables()["locations"]
    if loc_id not in locs.index or "state_province" not in locs.columns:
        return None
    v = locs.at[loc_id, "state_province"]
    return v if isinstance(v, str) and v.strip() else None


def rule_target(code_locs: set[str], eia_states) -> Target:
    """The automatic rule alone: one GEM location, in the mirror, same state."""
    if not code_locs:
        return Target(None, "no GEM code")
    if len(code_locs) > 1:
        return Target(None, f"GEM lists the code at {len(code_locs)} locations")
    (loc,) = code_locs
    if gemref.location(loc) is None:
        return Target(None, "GEM location not in the gem_locations mirror")
    eia = eia_state_name(eia_states)
    if eia is None:
        return Target(None, f"EIA state unknown or several ({eia_states})")
    gem = gem_state(loc)
    if gem is None:
        return Target(None, "GEM records no state for the location")
    if gem != eia:
        return Target(None, f"state differs: EIA {eia}, GEM {gem}")
    return Target(loc, "rule")


def load_review(path: Path = REVIEW_FILE) -> dict[str, dict]:
    """plant_code → review row; raises on an unknown verdict or a duplicated code."""
    if not path.exists():
        return {}
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    bad = sorted(set(df["verdict"]) - VERDICTS)
    if bad:
        raise ValueError(
            f"{path.name}: unknown verdicts {bad} (allowed: {sorted(VERDICTS)})"
        )
    dup = df["plant_code"][df["plant_code"].duplicated()]
    if len(dup):
        raise ValueError(f"{path.name}: plant codes listed twice: {dup.tolist()[:5]}")
    return {r["plant_code"]: r for r in df.to_dict("records")}


def target(
    code: str, code_locs: set[str], eia_states, review: dict[str, dict]
) -> Target:
    """Where the build links this EIA plant code by GEM's ID, if anywhere."""
    row = review.get(code)
    if row is not None:
        if row["verdict"] != "accept":
            return Target(None, f"review:{row['verdict']}")
        loc = row["gem_location_id"]
        if gemref.location(loc) is None:
            raise ValueError(
                f"{REVIEW_FILE.name}: accepted EIA {code} → {loc}, which is not in gem_locations"
            )
        return Target(loc, "review:accept")
    return rule_target(code_locs, eia_states)


def match_eia_by_gem_id(
    eia_plants: pd.DataFrame,
    code_locations: dict[str, set[str]],
    review: dict[str, dict],
    columns: list[str],
) -> pd.DataFrame:
    """Crosswalk rows for the EIA plants linked by GEM's code (one per plant_code).

    ``eia_plants`` has plant_code, plant_name and eia_state. Attributes
    (coordinates, coal type, capacity) are filled later by derive_from_gem,
    like a manual link's.
    """
    rows = []
    for r in eia_plants.drop_duplicates("plant_code").itertuples(index=False):
        code = str(r.plant_code)
        t = target(code, code_locations.get(code, set()), r.eia_state, review)
        if t.gem_location_id is None:
            continue
        info = gemref.location(t.gem_location_id)
        row = {c: None for c in columns}
        row.update(
            plant_name=r.plant_name,
            plant_code=code,
            source_system="EIA",
            gem_location_id=t.gem_location_id,
            ref_source="GEM",
            ref_matched_name=info["name"],
            matching_method=METHOD,
            confidence="high",
            reasoning=f"GEM lists EIA plant code {code} at this location ({t.reason})",
            not_in_gem=False,
        )
        rows.append(row)
    return pd.DataFrame(rows, columns=columns)


def veto_cross_state(rows: pd.DataFrame, eia_states: dict[str, str]) -> pd.DataFrame:
    """Null EIA name matches that put a plant on a GEM location in another state.

    Name similarity crosses state lines freely (Coal Creek, ND → Coleto Creek,
    TX; Biron Mill, WI → Brewton Mill, AL). Pipeline rows only; tier 0 then
    re-applies decisions, and verify's G-USSTATE lists decided rows that cross.
    """
    pipe = (
        (rows["source_system"] == "EIA")
        & rows["gem_location_id"].notna()
        & ~rows["matching_method"].isin({"manual", "legacy", METHOD})
        & rows["decided_by"].isna()
    )
    kill = []
    for i in rows.index[pipe]:
        eia = eia_state_name(eia_states.get(str(rows.at[i, "plant_code"])))
        gem = gem_state(rows.at[i, "gem_location_id"])
        if eia and gem and eia != gem:
            kill.append(i)
    if kill:
        cols = ["gem_location_id", "gem_unit_id", "ref_source", "ref_matched_name",
                "confidence", "matching_method", "latitude", "longitude", "coal_type",
                "combustion_tech", "capacity_mw"]  # fmt: skip
        rows.loc[kill, cols] = None
        rows.loc[kill, "note"] = (
            "auto-link vetoed: name match to a GEM site in another state — needs review"
        )
        logger.info(
            f"state veto: {len(kill)} EIA name matches to a GEM site in another state nulled for review"
        )
    return rows


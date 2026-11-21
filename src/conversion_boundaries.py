"""When a converted coal unit stopped being coal: the month boundary per US and Japan unit.

A coal unit that GEM records as converted to gas, oil or biomass gets two
periods in gem_unit_monthly (tracker #10): coal up to its last coal month, the
other fuel after, with the post-conversion months on GEM's twin unit. Example:
Montour 1 (EIA plant 3149, generator 1) burned coal until 2024-01, so
2019-01 … 2024-01 count as coal on GEM unit G100000107004 and 2024-02 on as gas
on G100001017952, whatever Form 860's single technology label says today.

The boundary comes from, in this order:

1. data/crosswalks/conversion_boundaries.csv, a reviewed list:
   * ``reviewed_month``: the last coal month, settled from unit-level evidence
     (US: EIA-923 boiler fuel through the EIA-860 boiler-generator links);
   * ``never_coal``: not coal in any month (Christine's review), no twin;
   * ``keep_as_is``: left as today, e.g. a unit co-firing coal with biomass;
2. GEM's own record: a retired coal unit with a ``conversion_to_id`` counts as
   coal until December of its retirement year (Steven's #22 rule: GEM gives
   years only, conversions count at the end of the year).

Only EIA and OCCTO rows get a boundary: their coal flag lives with the unit
(EIA's Form 860 technology, OCCTO's view filter), so the split happens in the
view. ENTSO-E and Chile label every source row, and their conversions are
reviewed exceptions in the ETL. The table's ``is_coal`` keeps its meaning
("a coal unit"); only gem_unit_monthly's ``is_coal`` is per month.
"""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path
from typing import Literal

import pandas as pd
from loguru import logger
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .utils import get_crosswalk_dir

DECISIONS_FILE = get_crosswalk_dir() / "conversion_boundaries.csv"
SOURCES = ("EIA", "OCCTO")
BOUNDARY_COLUMNS = [
    "converted_gem_unit_id",
    "coal_until_month",
    "boundary_source",
    "fuel_before",
    "fuel_after",
]
GEM_YEAR_END = "gem-year-end"
REVIEWED = "reviewed"
NEVER_COAL = "never-coal"
# A twin GEM files at another plant is skipped and reported; more than this
# many means the GEM data or this code changed, and the build stops.
MAX_TWINS_ELSEWHERE = 5
_MONTH = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
# The fuel each view reports after the boundary, from GEM's conversion_to_fuel.
_OTHER_FUEL = {
    "EIA": {
        "fossil gas": "Natural Gas Steam Turbine",
        "bioenergy": "Wood/Wood Waste Biomass",
        "fossil liquids": "Petroleum Liquids",
    },
    "OCCTO": {"fossil gas": "gas", "bioenergy": "biomass", "fossil liquids": "oil"},
}
_COAL_FUEL = {"EIA": "Conventional Steam Coal", "OCCTO": "coal"}


class BoundaryDecision(BaseModel):
    """One reviewed decision about a converted or non-coal unit."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    source_system: Literal["EIA", "OCCTO"] = Field(description="Source of the unit")
    plant_key: str = Field(
        min_length=1,
        description="The source's plant key (EIA plant code, OCCTO plant name)",
    )
    unit_key: str = Field(
        min_length=1, description="The source's unit key (EIA generator ID, OCCTO unit)"
    )
    treatment: Literal["reviewed_month", "never_coal", "keep_as_is"] = Field(
        description="reviewed_month: coal until coal_until_month; never_coal: not coal in any month; "
        "keep_as_is: no boundary, today's coal flag stays"
    )
    coal_until_month: str | None = Field(
        None,
        description="Last month the unit counts as coal (YYYY-MM); only for reviewed_month",
    )
    other_fuel: str | None = Field(
        None,
        description="Fuel the view reports for non-coal months; required for never_coal, "
        "otherwise taken from GEM's conversion_to_fuel",
    )
    reason: str = Field(
        min_length=1, description="The evidence, e.g. the EIA-923 coal share by year"
    )
    decided_by: str = Field(min_length=1, description="Who decided")
    decided_on: date = Field(description="When")
    ref: str = Field(
        min_length=1, description="Where it was discussed, e.g. a tracker issue"
    )

    @field_validator("coal_until_month", "other_fuel", mode="before")
    @classmethod
    def _blank_is_none(cls, v):
        return (
            None
            if v is None
            or (isinstance(v, float) and pd.isna(v))
            or str(v).strip() == ""
            else str(v)
        )

    @model_validator(mode="after")
    def _fields_match_treatment(self):
        if self.treatment == "reviewed_month":
            if not self.coal_until_month or not _MONTH.match(self.coal_until_month):
                raise ValueError(
                    f"{self.key}: reviewed_month needs coal_until_month as YYYY-MM"
                )
        elif self.coal_until_month:
            raise ValueError(f"{self.key}: coal_until_month is only for reviewed_month")
        if self.treatment == "never_coal" and not self.other_fuel:
            raise ValueError(f"{self.key}: never_coal needs other_fuel")
        return self

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.source_system, self.plant_key, self.unit_key)


def load_decisions(
    path: Path = DECISIONS_FILE,
) -> dict[tuple[str, str, str], BoundaryDecision]:
    """The reviewed decisions by (source_system, plant_key, unit_key); each unit once."""
    rows = pd.read_csv(path, dtype=str, keep_default_na=False).to_dict("records")
    decisions: dict[tuple[str, str, str], BoundaryDecision] = {}
    for row in rows:
        d = BoundaryDecision(**row)
        if d.key in decisions:
            raise ValueError(f"{path.name}: {' '.join(d.key)} listed twice")
        decisions[d.key] = d
    return decisions


def _other_fuel(source: str, conversion_fuel) -> str | None:
    first = str(conversion_fuel or "").split(",")[0].split(":")[0].strip()
    return _OTHER_FUEL[source].get(first, first or None)


def gem_conversions(gem_units: pd.DataFrame) -> pd.DataFrame:
    """Retired coal units with a GEM conversion twin, by gem_unit_id.

    Planned conversions (a retirement year GEM marks as planned, or no
    retirement year) are not conversions yet.
    """
    if "conversion_to_id" not in gem_units:
        return pd.DataFrame(
            columns=["conversion_to_id", "conversion_to_fuel", "retired_year"]
        )
    planned = gem_units["retired_year_is_planned"].astype("boolean").fillna(False)
    real = gem_units[
        (gem_units["tracker"] == "GCPT")
        & gem_units["conversion_to_id"].notna()
        & gem_units["retired_year"].notna()
        & ~planned
    ]
    return real.set_index("gem_unit_id")[
        ["conversion_to_id", "conversion_to_fuel", "retired_year"]
    ]


def apply_boundaries(
    unit_map: pd.DataFrame,
    gem_units: pd.DataFrame,
    decisions: dict[tuple[str, str, str], BoundaryDecision],
) -> pd.DataFrame:
    """unit_map with BOUNDARY_COLUMNS filled for EIA and OCCTO rows (NULL elsewhere).

    Raises ValueError when a reviewed decision names a unit the map lacks, or
    when more than MAX_TWINS_ELSEWHERE twins sit at another GEM plant.
    """
    out = unit_map.drop(columns=[c for c in BOUNDARY_COLUMNS if c in unit_map]).copy()
    for col in BOUNDARY_COLUMNS:
        out[col] = None
    keys = set(zip(out["source_system"], out["plant_key"], out["unit_key"]))
    missing = [" ".join(k) for k in decisions if k not in keys]
    if missing:
        raise ValueError(f"{DECISIONS_FILE.name}: not in the unit map: {missing}")

    conversions = gem_conversions(gem_units)
    location = dict(zip(gem_units["gem_unit_id"], gem_units["gem_location_id"]))
    elsewhere, counts = [], {GEM_YEAR_END: 0, REVIEWED: 0, NEVER_COAL: 0}
    for i, row in out[out["source_system"].isin(SOURCES)].iterrows():
        source = row["source_system"]
        decision = decisions.get((source, row["plant_key"], row["unit_key"]))
        if decision is not None and decision.treatment == "keep_as_is":
            continue
        conv = (
            conversions.loc[row["gem_unit_id"]]
            if row["gem_unit_id"] in conversions.index
            else None
        )
        twin = conv["conversion_to_id"] if conv is not None else None
        if twin is not None and location.get(twin) != row["gem_location_id"]:
            elsewhere.append(f"{source} {row['plant_key']} {row['unit_key']} -> {twin}")
            twin = None
            if decision is None:
                continue
        if decision is not None and decision.treatment == "never_coal":
            until, how, twin, after = None, NEVER_COAL, None, decision.other_fuel
        elif decision is not None:
            until, how = decision.coal_until_month, REVIEWED
            after = decision.other_fuel or (
                _other_fuel(source, conv["conversion_to_fuel"])
                if conv is not None
                else None
            )
        elif conv is not None:
            until, how = f"{int(conv['retired_year'])}-12", GEM_YEAR_END
            after = _other_fuel(source, conv["conversion_to_fuel"])
        else:
            continue
        if after is None and not row["is_coal"]:
            after = row["fuel"]
        coal_label = row["fuel"] if row["is_coal"] else _COAL_FUEL[source]
        out.loc[i, BOUNDARY_COLUMNS] = [
            twin,
            until,
            how,
            coal_label,
            after or row["fuel"],
        ]
        counts[how] += 1

    if len(elsewhere) > MAX_TWINS_ELSEWHERE:
        raise ValueError(
            f"{len(elsewhere)} GEM conversion twins sit at another GEM plant: {elsewhere[:10]}"
        )
    for line in elsewhere:
        logger.warning(
            f"conversion twin at another GEM plant, boundary skipped: {line}"
        )
    logger.info(
        f"conversion boundaries: {counts[REVIEWED]} reviewed, {counts[GEM_YEAR_END]} from GEM's year, "
        f"{counts[NEVER_COAL]} never coal; {len(elsewhere)} twins elsewhere skipped"
    )
    return out

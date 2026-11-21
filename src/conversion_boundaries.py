"""When a converted coal unit stopped being coal: the month boundary per US and Japan unit.

A coal unit that GEM records as converted to gas, oil or biomass gets two
periods in gem_unit_monthly (tracker #10): coal up to its last coal month, the
other fuel after, with the post-conversion months on GEM's twin unit. Example:
Montour 1 (EIA plant 3149, generator 1) burned coal until 2024-01, so
2019-01 … 2024-01 count as coal on GEM unit G100000107004 and 2024-02 on as gas
on G100001017952, whatever Form 860's single technology label says today.
Months before the boundary in which the unit burned no coal (Montour 2 ran on
gas in 2023-08 and 2023-10, then on coal again) are listed as non-coal months.

The boundary comes from, in this order:

1. data/crosswalks/conversion_boundaries.csv, a reviewed list:
   * ``reviewed_month``: the last coal month and the non-coal months before
     it, from unit-level evidence (US: EIA-923 boiler fuel through the
     EIA-860 boiler-generator links, coal at least half the heat input);
   * ``never_coal``: not coal in any month (Christine's review), no twin;
   * ``keep_as_is``: left as today, e.g. a unit co-firing coal with biomass;
2. GEM's own record: a retired coal unit with a ``conversion_to_id`` counts as
   coal until December of its retirement year (Steven's #22 rule: GEM gives
   years only, conversions count at the end of the year).

A reviewed month needs GEM's twin at the same GEM plant, or an ``other_fuel``
in the file; otherwise the row is marked ``unresolved`` and
verify_crosswalk.py's G-CONVERSION fails, instead of the view guessing.

Only EIA and OCCTO rows get a boundary: their coal flag lives with the unit
(EIA's Form 860 technology, OCCTO's view filter), so the split happens in the
view. ENTSO-E and Chile label every source row, and their conversions are
reviewed exceptions in the ETL. The table's ``is_coal`` keeps its meaning
("a coal unit"); only gem_unit_monthly's ``is_coal`` is per month.
"""

from __future__ import annotations

import re
from datetime import date
from enum import StrEnum
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
    "noncoal_months",
    "boundary_source",
    "fuel_before",
    "fuel_after",
]


class BoundarySource(StrEnum):
    """Where a unit-map row's boundary comes from (unit_gem_map.boundary_source)."""

    REVIEWED = "reviewed"  # conversion_boundaries.csv, reviewed_month
    GEM_YEAR_END = "gem-year-end"  # December of GEM's conversion year
    NEVER_COAL = "never-coal"  # conversion_boundaries.csv, never_coal
    # Not applied by the view; G-CONVERSION reports them:
    UNRESOLVED = "unresolved"  # a reviewed month without a twin or other_fuel
    TWIN_ELSEWHERE = "twin-elsewhere"  # GEM's twin sits at another GEM plant


# The boundaries gem_unit_monthly applies.
ACTIVE = (
    BoundarySource.REVIEWED,
    BoundarySource.GEM_YEAR_END,
    BoundarySource.NEVER_COAL,
)
_MONTH = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
# The fuel each view reports for non-coal months, from GEM's conversion_to_fuel
# ("fossil gas: natural gas", "fossil liquids: petroleum coke", ...): the
# subtype first, then the fuel family.
_OTHER_FUEL = {
    "EIA": {
        "petroleum coke": "Petroleum Coke",
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
    noncoal_months: tuple[str, ...] = Field(
        (),
        description="Months up to coal_until_month without coal (YYYY-MM, space separated in the file); "
        "only for reviewed_month",
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

    @field_validator("noncoal_months", mode="before")
    @classmethod
    def _split_months(cls, v):
        return (
            tuple(str(v or "").split())
            if not isinstance(v, (tuple, list))
            else tuple(v)
        )

    @model_validator(mode="after")
    def _fields_match_treatment(self):
        if self.treatment == "reviewed_month":
            if not self.coal_until_month or not _MONTH.match(self.coal_until_month):
                raise ValueError(
                    f"{self.key}: reviewed_month needs coal_until_month as YYYY-MM"
                )
            bad = [
                m
                for m in self.noncoal_months
                if not _MONTH.match(m) or m > self.coal_until_month
            ]
            if bad:
                raise ValueError(
                    f"{self.key}: noncoal_months must be YYYY-MM up to coal_until_month: {bad}"
                )
        elif self.coal_until_month or self.noncoal_months:
            raise ValueError(
                f"{self.key}: coal_until_month and noncoal_months are only for reviewed_month"
            )
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


def stale_decisions(
    unit_map: pd.DataFrame, decisions: dict[tuple[str, str, str], BoundaryDecision]
) -> list[str]:
    """Decisions for a source the map has, naming a unit the map lacks."""
    present = set(unit_map["source_system"])
    keys = set(
        zip(unit_map["source_system"], unit_map["plant_key"], unit_map["unit_key"])
    )
    return [" ".join(k) for k in decisions if k[0] in present and k not in keys]


def _other_fuel(source: str, conversion_fuel) -> str | None:
    """The view's fuel label for GEM's conversion_to_fuel; GEM's text when no label fits."""
    text = str(conversion_fuel or "").split(",")[0].strip()
    if not text:
        return None
    family, _, subtype = (part.strip() for part in text.partition(":"))
    labels = _OTHER_FUEL[source]
    if subtype in labels or family in labels:
        return labels.get(subtype) or labels[family]
    logger.warning(
        f"no {source} fuel label for GEM's conversion fuel {text!r}; kept as GEM writes it"
    )
    return text


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

    Never raises on data: a stale decision, an unresolved reviewed month or a
    twin at another GEM plant is logged here and fails G-CONVERSION in
    verify_crosswalk.py, where the weekly job stops before loading.
    """
    out = unit_map.drop(columns=[c for c in BOUNDARY_COLUMNS if c in unit_map]).copy()
    for col in BOUNDARY_COLUMNS:
        out[col] = None
    for stale in stale_decisions(out, decisions):
        logger.warning(f"{DECISIONS_FILE.name}: not in the unit map: {stale}")

    conversions = gem_conversions(gem_units)
    location = dict(zip(gem_units["gem_unit_id"], gem_units["gem_location_id"]))
    counts = dict.fromkeys(BoundarySource, 0)
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
        twin_here = twin is not None and location.get(twin) == row["gem_location_id"]
        coal_label = row["fuel"] if row["is_coal"] else _COAL_FUEL[source]
        until, noncoal, after = None, None, None
        if decision is not None and decision.treatment == "never_coal":
            how, twin, after = BoundarySource.NEVER_COAL, None, decision.other_fuel
        elif decision is not None:
            until, noncoal = (
                decision.coal_until_month,
                " ".join(decision.noncoal_months) or None,
            )
            after = decision.other_fuel or (
                _other_fuel(source, conv["conversion_to_fuel"]) if twin_here else None
            )
            how = (
                BoundarySource.REVIEWED
                if (twin_here or decision.other_fuel) and after
                else BoundarySource.UNRESOLVED
            )
            twin = twin if twin_here else None
        elif conv is not None:
            until, after = (
                f"{int(conv['retired_year'])}-12",
                _other_fuel(source, conv["conversion_to_fuel"]),
            )
            how = (
                BoundarySource.GEM_YEAR_END
                if twin_here and after
                else BoundarySource.TWIN_ELSEWHERE
            )
            twin = twin if twin_here else None
        else:
            continue
        out.loc[i, BOUNDARY_COLUMNS] = [
            twin,
            until,
            noncoal,
            str(how),
            coal_label,
            after,
        ]
        counts[how] += 1

    for how in (BoundarySource.UNRESOLVED, BoundarySource.TWIN_ELSEWHERE):
        for r in out[out["boundary_source"] == how].itertuples(index=False):
            logger.warning(
                f"conversion boundary not applied ({how}): {r.source_system} {r.plant_key} {r.unit_key}"
            )
    logger.info(
        "conversion boundaries: " + ", ".join(f"{n} {how}" for how, n in counts.items())
    )
    return out

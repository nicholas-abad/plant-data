"""Converted units split at their last coal month (src/conversion_boundaries.py, tracker #10)."""

import pandas as pd
import pytest
from pydantic import ValidationError

from src.conversion_boundaries import (
    BOUNDARY_COLUMNS,
    DECISIONS_FILE,
    BoundaryDecision,
    apply_boundaries,
    load_decisions,
    stale_decisions,
)
from src.unit_gem_map import COLUMNS

DECISION = dict(
    reason="EIA-923 boiler fuel",
    decided_by="N. Abad",
    decided_on="2026-11-21",
    ref="tracker#10",
    other_fuel="",
)


def _map(rows):
    base = dict(gem_unit_ids=None, map_source="gem-eia-id", other_gem_unit_ids=None)
    out = pd.DataFrame([{**base, **r} for r in rows])
    out["gem_unit_ids"] = out["gem_unit_ids"].fillna(out["gem_unit_id"])
    return out[COLUMNS]


UNITS = pd.DataFrame(
    [
        # gem_unit_id, location, tracker, retired, planned, twin, fuel
        ("G_MONTOUR1", "L_MONTOUR", "GCPT", 2024, False, "G_MONTOUR1_GAS", "fossil gas: natural gas"),
        ("G_MONTOUR1_GAS", "L_MONTOUR", "GOGPT", None, None, None, None),
        ("G_BRIDGER1", "L_BRIDGER", "GCPT", 2024, False, "G_BRIDGER1_GAS", "fossil gas: natural gas"),
        ("G_BRIDGER1_GAS", "L_BRIDGER", "GOGPT", None, None, None, None),
        ("G_SUZUKAWA", "L_SUZUKAWA", "GCPT", 2022, False, "G_SUZUKAWA_BIO", "bioenergy: wood & other biomass (solids)"),
        ("G_SUZUKAWA_BIO", "L_SUZUKAWA", "GBPT", None, None, None, None),
        ("G_PLANNED", "L_PLANNED", "GCPT", 2030, True, "G_PLANNED_GAS", "fossil gas: natural gas"),
        ("G_PLANNED_GAS", "L_PLANNED", "GOGPT", None, None, None, None),
        ("G_FAR", "L_FAR", "GCPT", 2021, False, "G_FAR_GAS", "fossil gas: natural gas"),
        ("G_FAR_GAS", "L_ELSEWHERE", "GOGPT", None, None, None, None),
    ],
    columns=["gem_unit_id", "gem_location_id", "tracker", "retired_year", "retired_year_is_planned",
             "conversion_to_id", "conversion_to_fuel"],
)  # fmt: skip

MAP = _map(
    [
        dict(source_system="EIA", plant_key="3149", unit_key="1", gem_location_id="L_MONTOUR",
             gem_unit_id="G_MONTOUR1", fuel="Conventional Steam Coal", is_coal=True),
        dict(source_system="EIA", plant_key="8066", unit_key="1", gem_location_id="L_BRIDGER",
             gem_unit_id="G_BRIDGER1", fuel="Natural Gas Steam Turbine", is_coal=False),
        dict(source_system="OCCTO", plant_key="鈴川", unit_key="単独", gem_location_id="L_SUZUKAWA",
             gem_unit_id="G_SUZUKAWA", fuel="coal", is_coal=True, map_source="single-unit"),
        dict(source_system="OCCTO", plant_key="石巻", unit_key="1号機", gem_location_id="L_ISHINOMAKI",
             gem_unit_id=None, fuel="coal", is_coal=True, map_source="crosswalk-plant"),
        dict(source_system="EIA", plant_key="9", unit_key="1", gem_location_id="L_PLANNED",
             gem_unit_id="G_PLANNED", fuel="Conventional Steam Coal", is_coal=True),
        dict(source_system="EIA", plant_key="7", unit_key="1", gem_location_id="L_FAR",
             gem_unit_id="G_FAR", fuel="Conventional Steam Coal", is_coal=True),
        dict(source_system="ENTSOE", plant_key="Studstrup 3", unit_key="", gem_location_id="L_STUDSTRUP",
             gem_unit_id="G_STUDSTRUP3", fuel="coal", is_coal=True, map_source="gipt-unit"),
    ]
)  # fmt: skip

DECISIONS = {
    d.key: d
    for d in (
        BoundaryDecision(source_system="EIA", plant_key="3149", unit_key="1", treatment="reviewed_month",
                         coal_until_month="2024-01", **DECISION),
        BoundaryDecision(source_system="EIA", plant_key="8066", unit_key="1", treatment="reviewed_month",
                         coal_until_month="2023-12", **DECISION),
        BoundaryDecision(source_system="OCCTO", plant_key="石巻", unit_key="1号機", treatment="never_coal",
                         **{**DECISION, "other_fuel": "biomass"}),
    )
}  # fmt: skip


def _row(out, source, plant):
    return out[(out["source_system"] == source) & (out["plant_key"] == plant)].iloc[0]


def test_a_reviewed_month_wins_over_gems_year_and_moves_to_the_twin():
    out = apply_boundaries(MAP, UNITS, DECISIONS)
    montour = _row(out, "EIA", "3149")
    assert (montour.coal_until_month, montour.boundary_source, montour.converted_gem_unit_id) == (
        "2024-01", "reviewed", "G_MONTOUR1_GAS",
    )  # fmt: skip
    assert (montour.fuel_before, montour.fuel_after) == (
        "Conventional Steam Coal",
        "Natural Gas Steam Turbine",
    )


def test_a_unit_form_860_relabelled_as_gas_gets_its_coal_label_back_before_the_boundary():
    bridger = _row(apply_boundaries(MAP, UNITS, DECISIONS), "EIA", "8066")
    assert (bridger.fuel_before, bridger.fuel_after) == (
        "Conventional Steam Coal",
        "Natural Gas Steam Turbine",
    )
    assert bridger.coal_until_month == "2023-12"


def test_without_a_reviewed_month_gems_conversion_year_ends_in_december():
    suzukawa = _row(apply_boundaries(MAP, UNITS, DECISIONS), "OCCTO", "鈴川")
    assert (suzukawa.coal_until_month, suzukawa.boundary_source) == (
        "2022-12",
        "gem-year-end",
    )
    assert (suzukawa.converted_gem_unit_id, suzukawa.fuel_after) == (
        "G_SUZUKAWA_BIO",
        "biomass",
    )


def test_a_never_coal_unit_has_no_twin_and_no_last_month():
    ishinomaki = _row(apply_boundaries(MAP, UNITS, DECISIONS), "OCCTO", "石巻")
    assert ishinomaki.boundary_source == "never-coal"
    assert (
        ishinomaki.coal_until_month is None and ishinomaki.converted_gem_unit_id is None
    )
    assert ishinomaki.fuel_after == "biomass"


def test_planned_conversions_and_other_sources_get_no_boundary():
    out = apply_boundaries(MAP, UNITS, DECISIONS)
    for source, plant in (("EIA", "9"), ("ENTSOE", "Studstrup 3")):
        assert _row(out, source, plant)[BOUNDARY_COLUMNS].isna().all()


def test_a_gem_twin_at_another_plant_is_marked_and_not_applied():
    far = _row(apply_boundaries(MAP, UNITS, DECISIONS), "EIA", "7")
    assert far.boundary_source == "twin-elsewhere" and far.converted_gem_unit_id is None


def test_a_reviewed_month_without_gems_twin_or_other_fuel_is_unresolved():
    units = UNITS[UNITS["gem_unit_id"] != "G_MONTOUR1"]
    montour = _row(apply_boundaries(MAP, units, DECISIONS), "EIA", "3149")
    assert (
        montour.boundary_source == "unresolved"
        and montour.converted_gem_unit_id is None
    )


def test_a_reviewed_month_with_its_own_other_fuel_needs_no_twin():
    gas = BoundaryDecision(
        source_system="EIA", plant_key="3149", unit_key="1", treatment="reviewed_month",
        coal_until_month="2024-01", **{**DECISION, "other_fuel": "Natural Gas Steam Turbine"},
    )  # fmt: skip
    units = UNITS[UNITS["gem_unit_id"] != "G_MONTOUR1"]
    montour = _row(
        apply_boundaries(MAP, units, {**DECISIONS, gas.key: gas}), "EIA", "3149"
    )
    assert (montour.boundary_source, montour.fuel_after) == (
        "reviewed",
        "Natural Gas Steam Turbine",
    )


def test_non_coal_months_before_the_boundary_are_carried():
    gaps = BoundaryDecision(
        source_system="EIA", plant_key="3149", unit_key="1", treatment="reviewed_month",
        coal_until_month="2024-02", noncoal_months="2023-08 2023-10", **DECISION,
    )  # fmt: skip
    montour = _row(
        apply_boundaries(MAP, UNITS, {**DECISIONS, gaps.key: gaps}), "EIA", "3149"
    )
    assert montour.noncoal_months == "2023-08 2023-10"


def test_petroleum_coke_keeps_its_own_label():
    units = UNITS.copy()
    units.loc[units["gem_unit_id"] == "G_MONTOUR1", "conversion_to_fuel"] = (
        "fossil liquids: petroleum coke"
    )
    no_decision = {k: v for k, v in DECISIONS.items() if k != ("EIA", "3149", "1")}
    assert (
        _row(apply_boundaries(MAP, units, no_decision), "EIA", "3149").fuel_after
        == "Petroleum Coke"
    )


def test_keep_as_is_stops_gems_year_from_applying():
    keep = BoundaryDecision(
        source_system="OCCTO",
        plant_key="鈴川",
        unit_key="単独",
        treatment="keep_as_is",
        **DECISION,
    )
    out = apply_boundaries(MAP, UNITS, {**DECISIONS, keep.key: keep})
    assert _row(out, "OCCTO", "鈴川")[BOUNDARY_COLUMNS].isna().all()


def test_a_stale_decision_is_reported_only_for_sources_the_map_has():
    ghost_eia = BoundaryDecision(
        source_system="EIA",
        plant_key="1",
        unit_key="X",
        treatment="never_coal",
        **{**DECISION, "other_fuel": "gas"},
    )
    eia_only = MAP[MAP["source_system"] == "EIA"]
    decisions = {**DECISIONS, ghost_eia.key: ghost_eia}
    assert stale_decisions(eia_only, decisions) == [
        "EIA 1 X"
    ]  # the OCCTO decision is not stale: no OCCTO rows
    apply_boundaries(eia_only, UNITS, decisions)  # logs, does not raise


def test_gem_units_loaded_before_the_conversion_columns_leave_reviewed_months_unresolved():
    # The reviewed month needs GEM's twin; without the detail pull it is reported, not guessed.
    out = apply_boundaries(
        MAP, UNITS.drop(columns=["conversion_to_id", "conversion_to_fuel"]), DECISIONS
    )
    assert _row(out, "OCCTO", "鈴川")[BOUNDARY_COLUMNS].isna().all()
    assert _row(out, "EIA", "3149").boundary_source == "unresolved"


@pytest.mark.parametrize(
    "fields, message",
    [
        (dict(treatment="reviewed_month"), "needs coal_until_month"),
        (
            dict(treatment="reviewed_month", coal_until_month="2024-13"),
            "needs coal_until_month",
        ),
        (
            dict(treatment="keep_as_is", coal_until_month="2024-01"),
            "only for reviewed_month",
        ),
        (dict(treatment="never_coal"), "needs other_fuel"),
        (
            dict(
                treatment="reviewed_month",
                coal_until_month="2024-01",
                noncoal_months="2024-03",
            ),
            "up to coal_until_month",
        ),
        (
            dict(treatment="keep_as_is", noncoal_months="2023-08"),
            "only for reviewed_month",
        ),
    ],
)
def test_a_decision_must_fit_its_treatment(fields, message):
    with pytest.raises(ValidationError, match=message):
        BoundaryDecision(
            source_system="EIA", plant_key="1", unit_key="1", **{**DECISION, **fields}
        )


def test_a_unit_listed_twice_fails(tmp_path):
    row = "EIA,1,1,keep_as_is,,,,co-firing,N. Abad,2026-11-21,tracker#10"
    path = tmp_path / "boundaries.csv"
    path.write_text(DECISIONS_FILE.read_text().splitlines()[0] + f"\n{row}\n{row}\n")
    with pytest.raises(ValueError, match="listed twice"):
        load_decisions(path)


def test_the_committed_list_loads():
    decisions = load_decisions()
    assert (
        decisions[("EIA", "8066", "1")].coal_until_month == "2023-12"
    )  # Jim Bridger 1
    assert decisions[("EIA", "3149", "2")].noncoal_months == (
        "2023-08",
        "2023-10",
    )  # Montour 2
    assert decisions[("OCCTO", "石巻雲雀野発電所", "1号機")].treatment == "never_coal"

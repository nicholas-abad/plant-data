"""unit_gem_map, US rows: each EIA generator's GEM unit (no database)."""

import pandas as pd
import pytest

from src.eia_unit_map import build_eia_unit_map, gem_candidates
from src.unit_gem_map import COLUMNS, with_fallback

UNITS = pd.DataFrame(
    [
        ("G_RP1", "GCPT", "operating"),  # Rockport Unit 1
        ("G_RP2", "GCPT", "operating"),
        ("G_BON_ADD", "GCPT", "cancelled"),  # Bonanza Addition, never built
        ("G_BON_1", "GCPT", "operating"),
        ("G_CONV_COAL", "GCPT", "retired"),  # a coal unit converted to gas ...
        ("G_CONV_GAS", "GOGPT", "operating"),  # ... and its gas-tracker twin
        ("G_TIE_A", "GCPT", "operating"),
        ("G_TIE_B", "GCPT", "operating"),
        ("G_GAS", "GOGPT", "operating"),
    ],
    columns=["gem_unit_id", "tracker", "status"],
)


def cands(rows):
    return pd.DataFrame(rows, columns=["plant_code", "generator_id", "gem_unit_id", "gem_location_id"])


def gens(*keys):
    df = pd.DataFrame(keys, columns=["plant_code", "generator_id"])
    return df.assign(technology="Conventional Steam Coal")


def test_one_unit_per_generator_maps_directly():
    out, miss = build_eia_unit_map(
        gens(("6166", "1"), ("6166", "2"), ("9", "GT1")),
        cands([("6166", "1", "G_RP1", "L_RP"), ("6166", "2", "G_RP2", "L_RP"), ("9", "GT1", "G_GAS", "L_9")]),
        UNITS,
    )
    assert dict(zip(out["unit_key"] + "@" + out["plant_key"], out["gem_unit_id"])) == {
        "1@6166": "G_RP1",
        "2@6166": "G_RP2",
        "GT1@9": "G_GAS",  # other trackers' units map too (multi-fuel sites)
    }
    assert miss.empty and set(out["map_source"]) == {"gem-eia-id"}
    assert list(out.columns) == COLUMNS and set(out["source_system"]) == {"EIA"}
    assert out["is_coal"].all() and set(out["fuel"]) == {"Conventional Steam Coal"}


def test_never_built_unit_gives_way():
    out, _ = build_eia_unit_map(
        gens(("7790", "1")),
        cands([("7790", "1", "G_BON_ADD", "L_BON"), ("7790", "1", "G_BON_1", "L_BON")]),
        UNITS,
    )
    assert out["gem_unit_id"].tolist() == ["G_BON_1"]
    assert out["other_gem_unit_ids"].tolist() == ["G_BON_ADD"]


def test_coal_unit_wins_over_its_other_tracker_twin():
    out, _ = build_eia_unit_map(
        gens(("5", "3")),
        cands([("5", "3", "G_CONV_GAS", "L_5"), ("5", "3", "G_CONV_COAL", "L_5")]),
        UNITS,
    )
    assert out["gem_unit_id"].tolist() == ["G_CONV_COAL"]
    assert out["other_gem_unit_ids"].tolist() == ["G_CONV_GAS"]


def test_a_real_tie_is_left_unmapped_not_guessed():
    out, miss = build_eia_unit_map(
        gens(("8", "1")),
        cands([("8", "1", "G_TIE_A", "L_8"), ("8", "1", "G_TIE_B", "L_8")]),
        UNITS,
    )
    assert out.empty
    assert "G_TIE_A, G_TIE_B" in miss["reason"].iloc[0]


def test_generators_without_generation_are_not_mapped():
    out, _ = build_eia_unit_map(gens(("6166", "1")), cands([("6166", "2", "G_RP2", "L_RP")]), UNITS)
    assert out.empty


def test_candidates_pair_units_with_their_locations_plant_codes():
    ids = pd.DataFrame(
        [
            ("L_RP", None, "EIA", "6166"),
            ("L_RP", "G_RP1", "EIA", "1"),
            ("L_RP", "G_RP1", "Beyond Fossil Fuels", "X"),
            ("L_TWO", None, "EIA", "100"),
            ("L_TWO", None, "EIA", "200"),
            ("L_TWO", "G_TIE_A", "EIA", "1"),
        ],
        columns=["gem_location_id", "gem_unit_id", "namespace", "external_id"],
    )
    got = gem_candidates(ids).sort_values(["plant_code"]).values.tolist()
    assert got == [
        ["L_TWO", "G_TIE_A", "1", "100"],
        ["L_TWO", "G_TIE_A", "1", "200"],
        ["L_RP", "G_RP1", "1", "6166"],
    ]


def test_units_missing_from_the_mirror_wait():
    out, miss = build_eia_unit_map(
        gens(("6166", "1")), cands([("6166", "1", "G_NOT_MIRRORED", "L_RP")]), UNITS
    )
    assert out.empty and miss.empty


def test_unmatched_generators_fall_back_to_their_plants_gem_plant():
    matched, _ = build_eia_unit_map(
        gens(("6166", "1")), cands([("6166", "1", "G_RP1", "L_RP")]), UNITS
    )
    every = pd.DataFrame(
        {
            "source_system": "EIA",
            "plant_key": ["6166", "6166", "99", "8"],
            "unit_key": ["1", "GT9", "1", "1"],
            "fuel": ["Conventional Steam Coal", "Natural Gas Fired Combustion Turbine", None, None],
            "is_coal": [True, False, False, False],
        }
    )
    out = with_fallback(matched, every, {"6166": "L_RP", "8": "L_8"})
    rows = {(r.plant_key, r.unit_key): (r.gem_location_id, r.gem_unit_id, r.map_source) for r in out.itertuples()}
    assert rows == {
        ("6166", "1"): ("L_RP", "G_RP1", "gem-eia-id"),
        ("6166", "GT9"): ("L_RP", None, "crosswalk-plant"),
        ("8", "1"): ("L_8", None, "crosswalk-plant"),  # e.g. a tie: on its plant, no unit
    }  # plant 99 is linked to no GEM plant, so it is on no page


def test_ons_ids_place_every_name_and_phases_keep_all_their_units():
    from src.reviewed_unit_map import SOURCES, build_rows

    names = pd.DataFrame(
        [
            ("Jorge Lacerda IV", "SCUTLC"),
            ("U.JLACERDA C", "SCUTLC"),
            ("Porto do Pecém I", "CEUTPC"),
            ("Figueira", "PRUFRA"),
            ("São Jerônimo", "RSUSJE"),
        ],
        columns=["plant", "source_id"],
    )
    reviewed = pd.DataFrame(
        [("SCUTLC", "L_JL", "G7"), ("CEUTPC", "L_PEC", "G1 G2")],
        columns=["source_id", "gem_location_id", "gem_unit_ids"],
    )
    ons = next(src for src in SOURCES if src.system == "ONS")
    out = build_rows(ons, names, reviewed, {"Figueira": "L_FIG"}).set_index("plant_key")
    # both names of one ONS ID -> one GEM unit (a rename is one series)
    assert out.loc["Jorge Lacerda IV", "gem_unit_id"] == out.loc["U.JLACERDA C", "gem_unit_id"] == "G7"
    # a phase: no single unit, both kept
    assert pd.isna(out.loc["Porto do Pecém I", "gem_unit_id"])
    assert out.loc["Porto do Pecém I", "gem_unit_ids"] == "G1 G2"
    # unreviewed but linked: plant level; neither: no row
    assert out.loc["Figueira", "map_source"] == "crosswalk-plant"
    assert "São Jerônimo" not in out.index
    assert out["is_coal"].all() and set(out["unit_key"]) == {""}


def test_reviewed_files_name_real_units_at_their_plants():
    """Every committed reviewed row is well formed (the build checks units against gem_units)."""
    from src.reviewed_unit_map import SOURCES, load_reviewed

    for source in SOURCES:
        df = load_reviewed(source)
        assert len(df) and df["gem_location_id"].str.startswith("L").all()
        assert df["gem_unit_ids"].str.split().map(lambda ids: all(i.startswith("G") for i in ids)).all()


def test_oe_units_match_by_duid_then_reviewed_then_fall_back():
    from src.oe_unit_map import build_oe_unit_map

    units = pd.DataFrame(
        [("BW01", "Bayswater", "coal_black"), ("LD01", "Liddell", "coal_black"),
         ("XX9", "Bayswater", "gas_ocgt"), ("ZZ1", "Unlinked", "coal_black")],
        columns=["unit_code", "facility_name", "fueltech"],
    )  # fmt: skip
    candidates = pd.DataFrame(
        [("BW01", "G_BW1", "L_BW")], columns=["unit_code", "gem_unit_id", "gem_location_id"]
    )
    reviewed = pd.DataFrame(
        [("LD01", "L_LD", "G_LD1")], columns=["unit_code", "gem_location_id", "gem_unit_ids"]
    )
    gem = pd.DataFrame(
        [("G_BW1", "L_BW", "operating"), ("G_LD1", "L_LD", "retired")],
        columns=["gem_unit_id", "gem_location_id", "status"],
    )
    out = build_oe_unit_map(units, candidates, reviewed, gem, {"Bayswater": "L_BW"}).set_index("unit_key")
    assert out.loc["BW01", ["gem_unit_id", "map_source"]].tolist() == ["G_BW1", "gem-duid"]
    assert out.loc["LD01", ["gem_unit_id", "map_source"]].tolist() == ["G_LD1", "source-id-reviewed"]
    assert out.loc["XX9", "map_source"] == "crosswalk-plant" and not out.loc["XX9", "is_coal"]
    assert "ZZ1" not in out.index  # its facility is linked to no GEM plant


def test_occto_numbers_read_like_gems():
    from src.occto_unit_map import gem_number, occto_number

    assert [occto_number(u) for u in ["１号機", "新地1号機", "6号", "常陸那珂２号", "単独"]] == ["1", "1", "6", "2", None]
    assert [gem_number(n) for n in ["Unit 1", "Unit 3, timepoint1", "Phase B Unit 2", "--"]] == ["1", "3", "2", None]


def test_occto_units_match_by_number_single_unit_or_review():
    from src.occto_unit_map import build_occto_unit_map

    units = pd.DataFrame(
        [("Misumi", "１号機", "coal"), ("Misumi", "２号機", "coal"), ("Solo", "単独", "coal"),
         ("Takehara", "1号機", "coal"), ("Gasplant", "１号機", "gas")],
        columns=["plant", "unit", "fuel_type"],
    )  # fmt: skip
    gem = pd.DataFrame(
        [("G_M1", "L_M", "GCPT", "Unit 1", "operating"), ("G_M2", "L_M", "GCPT", "Unit 2", "operating"),
         ("G_S", "L_S", "GCPT", "--", "operating"), ("G_T1", "L_T", "GCPT", "Unit 1", "retired"),
         ("G_TN", "L_T", "GCPT", "New Unit 1", "operating")],
        columns=["gem_unit_id", "gem_location_id", "tracker", "unit_name", "status"],
    )  # fmt: skip
    links = {"Misumi": "L_M", "Solo": "L_S", "Takehara": "L_T", "Gasplant": "L_M"}
    out = build_occto_unit_map(units, pd.DataFrame(columns=["plant", "unit", "gem_location_id", "gem_unit_ids"]), gem, links)
    got = {(r.plant_key, r.unit_key): (r.gem_unit_id, r.map_source) for r in out.itertuples()}
    assert got[("Misumi", "１号機")] == ("G_M1", "unit-number")
    assert got[("Solo", "単独")] == ("G_S", "single-unit")
    # 'Unit 1' and 'New Unit 1' both read as 1: not settled by number, waits for review
    assert got[("Takehara", "1号機")] == (None, "crosswalk-plant")
    assert got[("Gasplant", "１号機")] == (None, "crosswalk-plant")
    reviewed = pd.DataFrame([("Takehara", "1号機", "L_T", "G_TN")], columns=["plant", "unit", "gem_location_id", "gem_unit_ids"])
    out = build_occto_unit_map(units, reviewed, gem, links).set_index(["plant_key", "unit_key"])
    assert out.loc[("Takehara", "1号機"), "gem_unit_id"] == "G_TN"


def test_occto_automatic_match_never_shares_a_gem_unit_between_plants():
    from src.occto_unit_map import build_occto_unit_map

    units = pd.DataFrame([("Hibiki A", "単独", "coal"), ("Hibiki B", "1号機", "coal")], columns=["plant", "unit", "fuel_type"])
    gem = pd.DataFrame([("G_H", "L_H", "GCPT", "1, timepoint 1", "retired")],
                       columns=["gem_unit_id", "gem_location_id", "tracker", "unit_name", "status"])  # fmt: skip
    out = build_occto_unit_map(
        units, pd.DataFrame(columns=["plant", "unit", "gem_location_id", "gem_unit_ids"]), gem,
        {"Hibiki A": "L_H", "Hibiki B": "L_H"},
    )  # fmt: skip
    assert out["gem_unit_ids"].isna().all() and set(out["map_source"]) == {"crosswalk-plant"}


def test_occto_reviewed_unit_frees_the_number_match_for_the_other_plant():
    # Kyushu's and J-POWER's Matsuura both linked to J-POWER's GEM plant; the
    # reviewed rows move Kyushu's units, so J-POWER's match by number again.
    from src.occto_unit_map import build_occto_unit_map

    units = pd.DataFrame([("Kyushu", "１号機", "coal"), ("JPower", "1号機", "coal")], columns=["plant", "unit", "fuel_type"])
    gem = pd.DataFrame([("G_K1", "L_K", "GCPT", "Unit 1", "operating"), ("G_J1", "L_J", "GCPT", "Unit 1", "operating")],
                       columns=["gem_unit_id", "gem_location_id", "tracker", "unit_name", "status"])  # fmt: skip
    links = {"Kyushu": "L_J", "JPower": "L_J"}
    no_review = pd.DataFrame(columns=["plant", "unit", "gem_location_id", "gem_unit_ids"])
    assert build_occto_unit_map(units, no_review, gem, links)["gem_unit_ids"].isna().all()
    reviewed = pd.DataFrame([("Kyushu", "１号機", "L_K", "G_K1")], columns=no_review.columns)
    out = build_occto_unit_map(units, reviewed, gem, links).set_index("plant_key")
    assert out.loc["Kyushu", "gem_unit_ids"] == "G_K1" and out.loc["JPower", "gem_unit_ids"] == "G_J1"
    assert out.loc["JPower", "map_source"] == "unit-number"


def test_entsoe_names_read_like_gems():
    from src.entsoe_unit_map import entsoe_keys, gem_key

    assert entsoe_keys("Neurath G") == [(None, "G")]
    assert entsoe_keys("TE Kostolac A G1") == [("A", "1"), (None, "1")]
    assert entsoe_keys("Kozienice 2 B11") == [("2", "11"), (None, "11")]
    assert [entsoe_keys(n)[-1] for n in ["ELED_G6___", "PTOLEMAIDA5", "CTE_Rovinari_ROVI5_CA", "MÁ2_gép4", "LADA IV"]] == [
        (None, "6"), (None, "5"), (None, "5"), (None, "4"), (None, "4")
    ]  # fmt: skip
    assert entsoe_keys("TPP_MI2") == []  # a plant code, not a unit
    assert [gem_key(n) for n in ["Unit G", "Phase A Unit 1", "Unit I-3", "Unit 5, timepoint 1", "Phase B", "CHP 2", "--"]] == [
        (None, "G"), ("A", "1"), ("1", "3"), (None, "5"), (None, "B"), (None, "2"), (None, None)
    ]  # fmt: skip


def test_entsoe_units_match_within_phase_and_never_share_a_generating_unit():
    from src.entsoe_unit_map import build_entsoe_unit_map

    gem = pd.DataFrame(
        [("G_A1", "L_K", "GCPT", "Phase A Unit 1", "operating"), ("G_B1", "L_K", "GCPT", "Phase B Unit 1", "operating"),
         ("G_II1", "L_P", "GCPT", "Unit II-1", "operating"), ("G_S", "L_S", "GCPT", "--", "operating")],
        columns=["gem_unit_id", "gem_location_id", "tracker", "unit_name", "status"],
    )  # fmt: skip
    months = pd.DataFrame(
        [("Kostolac A G1", "2025-01"), ("Kostolac B G1", "2025-01"), ("Patnow 1 B1", "2010-01"),
         ("Two X", "2025-01"), ("Two X G1", "2025-01")],
        columns=["plant_name", "month"],
    )  # fmt: skip
    links = {"Kostolac A G1": "L_K", "Kostolac B G1": "L_K", "Patnow 1 B1": "L_P", "Two X": "L_S", "Two X G1": "L_S"}
    no_review = pd.DataFrame(columns=["plant_name", "gem_location_id", "gem_unit_ids"])
    out = build_entsoe_unit_map(months, no_review, gem, links).set_index("plant_key")
    assert out.loc["Kostolac A G1", "gem_unit_id"] == "G_A1" and out.loc["Kostolac B G1", "gem_unit_id"] == "G_B1"
    assert pd.isna(out.loc["Patnow 1 B1", "gem_unit_ids"])  # phase I unit 1 is not Unit II-1
    assert out.loc[["Two X", "Two X G1"], "map_source"].eq("crosswalk-plant").all()  # two names, one GEM unit
    reviewed = pd.DataFrame([("Two X G1", "L_S", "G_S")], columns=no_review.columns)
    out = build_entsoe_unit_map(months, reviewed, gem, links).set_index("plant_key")
    assert out.loc["Two X G1", "gem_unit_id"] == "G_S" and out.loc["Two X", "map_source"] == "crosswalk-plant"


def test_eia_unit_candidates_follow_the_plant_code_review():
    from src.eia_unit_map import apply_review

    cands = pd.DataFrame(
        [("10307", "1", "G_T", "L_TORONTO"), ("1", "1", "G_A", "L_A"), ("2", "1", "G_B", "L_B"), ("2", "2", "G_C", "L_C")],
        columns=["plant_code", "gem_generator_id", "gem_unit_id", "gem_location_id"],
    )  # fmt: skip
    review = {"10307": {"verdict": "defer", "gem_location_id": "L_TORONTO"}, "2": {"verdict": "accept", "gem_location_id": "L_B"}}
    kept = apply_review(cands, review)
    assert sorted(kept["gem_unit_id"]) == ["G_A", "G_B"]  # deferred code out; accepted code only at its location


def test_petroleum_coke_counts_as_coal():
    from src.eia_unit_map import COAL_TECHNOLOGIES

    assert "Petroleum Coke" in COAL_TECHNOLOGIES


def test_reviewed_claims_count_in_the_japan_and_europe_guards():
    from src.entsoe_unit_map import build_entsoe_unit_map
    from src.occto_unit_map import build_occto_unit_map

    gem = pd.DataFrame([("G1", "L1", "GCPT", "Unit 1", "operating")],
                       columns=["gem_unit_id", "gem_location_id", "tracker", "unit_name", "status"])  # fmt: skip
    units = pd.DataFrame([("A", "1号機", "coal"), ("B", "1号機", "coal")], columns=["plant", "unit", "fuel_type"])
    reviewed = pd.DataFrame([("A", "1号機", "L1", "G1")], columns=["plant", "unit", "gem_location_id", "gem_unit_ids"])
    out = build_occto_unit_map(units, reviewed, gem, {"A": "L1", "B": "L1"}).set_index("plant_key")
    assert out.loc["A", "map_source"] == "source-id-reviewed" and out.loc["B", "map_source"] == "crosswalk-plant"

    months = pd.DataFrame([("X G1", "2025-01"), ("Y G1", "2025-01")], columns=["plant_name", "month"])
    reviewed = pd.DataFrame([("X G1", "L1", "G1")], columns=["plant_name", "gem_location_id", "gem_unit_ids"])
    out = build_entsoe_unit_map(months, reviewed, gem, {"X G1": "L1", "Y G1": "L1"}).set_index("plant_key")
    assert out.loc["X G1", "map_source"] == "source-id-reviewed" and out.loc["Y G1", "map_source"] == "crosswalk-plant"


def test_a_reviewed_file_listing_a_unit_twice_is_refused():
    from pathlib import Path

    from src.unit_gem_map import check_reviewed

    gem = pd.DataFrame([("G1", "L1")], columns=["gem_unit_id", "gem_location_id"])
    twice = pd.DataFrame([("X", "L1", "G1"), ("X", "L1", "G1")], columns=["plant_name", "gem_location_id", "gem_unit_ids"])
    with pytest.raises(ValueError, match="listed twice"):
        check_reviewed(Path("x.csv"), twice, gem, ["plant_name"])

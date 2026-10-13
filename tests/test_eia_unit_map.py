"""eia_unit_gem_map: each EIA generator's GEM unit (no database)."""

import pandas as pd

from src.eia_unit_map import build_eia_unit_map, gem_candidates

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
    return pd.DataFrame(keys, columns=["plant_code", "generator_id"])


def test_one_unit_per_generator_maps_directly():
    out, miss = build_eia_unit_map(
        gens(("6166", "1"), ("6166", "2"), ("9", "GT1")),
        cands([("6166", "1", "G_RP1", "L_RP"), ("6166", "2", "G_RP2", "L_RP"), ("9", "GT1", "G_GAS", "L_9")]),
        UNITS,
    )
    assert dict(zip(out["generator_id"] + "@" + out["plant_code"], out["gem_unit_id"])) == {
        "1@6166": "G_RP1",
        "2@6166": "G_RP2",
        "GT1@9": "G_GAS",  # other trackers' units map too (multi-fuel sites)
    }
    assert miss.empty and set(out["map_source"]) == {"gem-eia-id"}


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

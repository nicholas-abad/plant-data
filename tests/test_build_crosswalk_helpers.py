"""Tests for the crosswalk builder's pure matching helpers."""

from src.build_crosswalk import (
    _clean_llm_match,
    _is_npp_likely_non_coal,
    _normalize_confidence,
)
from src.plant_name_matchers.normalizers import (
    build_norm_index as _build_norm_index,
    normalize_for_comparison,
)


class TestBuildNormIndex:
    def test_collision_keeps_first_deterministically(self):
        # "Foo power station" and "Foo power plant" both normalize to "FOO".
        # A plain dict comprehension kept whichever iterated LAST — i.e. an
        # arbitrary plant's coordinates. We keep the first, deterministically.
        idx = _build_norm_index(
            ["Foo power station", "Foo power plant"],
            normalize_for_comparison,
            "test",
        )
        assert idx == {"FOO": "Foo power station"}

    def test_empty_normalizations_are_excluded(self):
        # A pure parenthetical/punctuation name normalizes to "" — rapidfuzz
        # scores two empty strings 100, so an empty key would match any
        # empty-normalizing query.
        idx = _build_norm_index(
            ["(Liq.)", "---", "Korba power station"],
            normalize_for_comparison,
            "test",
        )
        assert "" not in idx
        assert idx == {"KORBA": "Korba power station"}

    def test_identical_duplicates_are_not_collisions(self):
        idx = _build_norm_index(
            ["Korba power station", "Korba power station"],
            normalize_for_comparison,
            "test",
        )
        assert idx == {"KORBA": "Korba power station"}


class TestCleanLlmMatch:
    def test_prefix_is_authoritative_and_stripped(self):
        source, name = _clean_llm_match("GEM: Korba power station")
        assert source == "GEM"
        assert name == "Korba power station"

    def test_echoed_score_suffix_is_stripped(self):
        # The candidates are formatted "GEM: name (score: 95)" — an obedient
        # LLM echoes that verbatim, which used to miss the coordinate lookup.
        source, name = _clean_llm_match("GPPD: Tuticorin (score: 87)")
        assert source == "GPPD"
        assert name == "Tuticorin"

    def test_bare_name_passes_through(self):
        source, name = _clean_llm_match("Korba power station")
        assert source is None
        assert name == "Korba power station"


class TestNormalizeConfidence:
    def test_capitalized_confidence_is_accepted(self):
        # "High" used to silently fail the ("high", "medium") membership test.
        assert _normalize_confidence("High") == "high"
        assert _normalize_confidence(" MEDIUM ") == "medium"

    def test_non_string_is_none(self):
        assert _normalize_confidence(None) is None
        assert _normalize_confidence(0.9) is None


class TestNppNonCoalSuffix:
    def test_hydro_suffix_detected(self):
        assert _is_npp_likely_non_coal("BHADRA HPS") is True

    def test_plain_coal_plant_not_flagged(self):
        assert _is_npp_likely_non_coal("KORBA STPS") is False

    def test_non_string_safe(self):
        assert _is_npp_likely_non_coal(None) is False


class TestAtomicReplaceGuard:
    def test_empty_dataframe_refused_before_touching_engine(self):
        import pandas as pd
        import pytest

        import sys
        from pathlib import Path

        sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
        from bootstrap_neon_db import _atomic_replace_table

        with pytest.raises(RuntimeError, match="0 rows"):
            # engine=None proves the guard fires before ANY engine use
            _atomic_replace_table(None, pd.DataFrame(), "plant_crosswalk", [])


class TestNonStringMatchGuard:
    """An LLM that returns a non-string `match` (dict/list/number from a
    malformed response) must not crash the whole paid LLM stage."""

    def test_clean_llm_match_raises_on_non_string(self):
        # documents the hazard: this is why match_llm guards with isinstance
        import pytest

        with pytest.raises(AttributeError):
            _clean_llm_match({"unexpected": "dict"})

    def test_usable_llm_match_filters_non_strings_and_low_confidence(self):
        # exercises the REAL guard helper used at the match_llm call site
        from src.build_crosswalk import _usable_llm_match

        assert _usable_llm_match("GEM: Foo", "high")
        assert _usable_llm_match("GEM: Foo", "medium")
        assert not _usable_llm_match("GEM: Foo", "low"), "low confidence rejected"
        assert not _usable_llm_match("", "high"), "empty string rejected"
        for bad in ({"a": 1}, ["x"], 42, None):
            assert not _usable_llm_match(bad, "high"), f"{bad!r} must be rejected"


class TestNppSuppressCoalMetadata:
    """Fuel-aware suppression: the DGR-2 fuel section beats the name heuristic."""

    def test_thermal_keeps_coal_metadata(self):
        from src.build_crosswalk import _npp_suppress_coal_metadata

        assert _npp_suppress_coal_metadata("NIGRI TPP", "THERMAL") is False
        # Fuel wins even when the name would trip the heuristic
        assert _npp_suppress_coal_metadata("SOMETHING CCPP", "THERMAL") is False

    def test_non_thermal_suppresses(self):
        from src.build_crosswalk import _npp_suppress_coal_metadata

        # The exact miss that motivated this: hydro PSP without an HPS suffix
        assert _npp_suppress_coal_metadata("GANDHI SAGAR PSP", "HYDRO") is True
        assert _npp_suppress_coal_metadata("KUDANKULAM", "NUCLEAR") is True
        assert _npp_suppress_coal_metadata("TRIPURA CCPP", "THER (GT)") is True

    def test_null_fuel_falls_back_to_name_heuristic(self):
        import numpy as np

        from src.build_crosswalk import (
            _is_npp_likely_non_coal,
            _npp_suppress_coal_metadata,
        )

        for null in (None, float("nan"), np.nan, ""):
            assert _npp_suppress_coal_metadata(
                "NATHPA JHAKRI HPS", null
            ) is _is_npp_likely_non_coal("NATHPA JHAKRI HPS")
            assert _npp_suppress_coal_metadata(
                "RAJPURA TPP", null
            ) is _is_npp_likely_non_coal("RAJPURA TPP")


class TestIterMatchGroups:
    def test_entsoe_splits_per_country_others_pass_through(self):
        import pandas as pd

        from src.build_crosswalk import SOURCE_COUNTRIES, _iter_match_groups

        df = pd.DataFrame(
            {
                "plant_name": ["cz1", "cz2", "de1", "india1"],
                "source_system": ["ENTSOE", "ENTSOE", "ENTSOE", "NPP"],
                "ref_gem_country": ["Czech Republic", "Czech Republic", "Germany", None],
                "ref_gppd_country": ["CZE", "CZE", "DEU", None],
            }
        )
        groups = list(_iter_match_groups(df))
        by_label = {label: (sub, gem_c, gppd_cs) for _, label, sub, gem_c, gppd_cs in groups}

        assert set(by_label) == {"ENTSOE/Czech Republic", "ENTSOE/Germany", "NPP"}
        sub, gem_c, gppd_cs = by_label["ENTSOE/Czech Republic"]
        assert len(sub) == 2 and gem_c == "Czech Republic" and gppd_cs == ["CZE"]
        sub, gem_c, gppd_cs = by_label["ENTSOE/Germany"]
        assert list(sub["plant_name"]) == ["de1"] and gppd_cs == ["DEU"]
        # Non-ENTSOE keeps its configured country refs
        _, gem_c, gppd_cs = by_label["NPP"]
        assert gem_c is None and gppd_cs == [SOURCE_COUNTRIES["NPP"]["gppd"]]

    def test_entsoe_without_country_column_stays_single_group(self):
        import pandas as pd

        from src.build_crosswalk import _iter_match_groups

        df = pd.DataFrame(
            {"plant_name": ["a", "b"], "source_system": ["ENTSOE", "ENTSOE"]}
        )
        groups = list(_iter_match_groups(df))
        assert len(groups) == 1 and groups[0][1] == "ENTSOE"


class TestDivideEntsoeSiteCapacity:
    def test_site_capacity_divided_across_matched_units(self):
        import pandas as pd

        from src.build_crosswalk import _divide_entsoe_site_capacity

        rows = pd.DataFrame(
            {
                "plant_name": ["NEURATH_A", "NEURATH_B", "NEURATH_C", "LONE_UNIT", "occto1"],
                "source_system": ["ENTSOE", "ENTSOE", "ENTSOE", "ENTSOE", "OCCTO"],
                "ref_source": ["GEM", "GEM", "GEM", "GEM", "GEM"],
                "ref_matched_name": [
                    "Neurath power station",
                    "Neurath power station",
                    "Neurath power station",
                    "Solo power station",
                    "Neurath power station",
                ],
                "capacity_mw": [4424.0, 4424.0, 4424.0, 600.0, 4424.0],
            }
        )
        # No generation info -> equal division fallback
        out = _divide_entsoe_site_capacity(rows.copy())

        neurath = out[out["ref_matched_name"] == "Neurath power station"]
        entsoe_units = neurath[neurath["source_system"] == "ENTSOE"]
        # Per-site sum equals the reference nameplate exactly
        import numpy as np

        assert np.isclose(entsoe_units["capacity_mw"].sum(), 4424.0)
        assert np.allclose(entsoe_units["capacity_mw"], 4424.0 / 3)
        # Single-unit sites and non-ENTSO-E rows untouched
        assert out.loc[out["plant_name"] == "LONE_UNIT", "capacity_mw"].item() == 600.0
        assert out.loc[out["plant_name"] == "occto1", "capacity_mw"].item() == 4424.0

    def test_generation_share_weighting(self):
        import pandas as pd

        from src.build_crosswalk import _divide_entsoe_site_capacity

        rows = pd.DataFrame(
            {
                "plant_name": ["NEURATH_A", "NEURATH_F", "NEURATH_G"],
                "source_system": ["ENTSOE"] * 3,
                "ref_source": ["GEM"] * 3,
                "ref_matched_name": ["Neurath power station"] * 3,
                "capacity_mw": [4424.0] * 3,
            }
        )
        # A retired (tiny lifetime share), F and G carry the generation.
        gen = {"NEURATH_A": 1_000_000.0, "NEURATH_F": 5_000_000.0, "NEURATH_G": 4_000_000.0}
        out = _divide_entsoe_site_capacity(rows, gen)

        assert out["capacity_mw"].sum() == 4424.0
        assert out.loc[out["plant_name"] == "NEURATH_A", "capacity_mw"].item() == 4424.0 * 0.1
        assert out.loc[out["plant_name"] == "NEURATH_F", "capacity_mw"].item() == 4424.0 * 0.5
        # Units with generation get capacity proportional to it -> unit CF == site CF

    def test_null_capacity_and_unmatched_rows_untouched(self):
        import pandas as pd

        from src.build_crosswalk import _divide_entsoe_site_capacity

        rows = pd.DataFrame(
            {
                "plant_name": ["u1", "u2"],
                "source_system": ["ENTSOE", "ENTSOE"],
                "ref_source": [None, "GEM"],
                "ref_matched_name": [None, "X power station"],
                "capacity_mw": [None, None],
            }
        )
        out = _divide_entsoe_site_capacity(rows)
        assert out["capacity_mw"].isna().all()

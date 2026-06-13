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

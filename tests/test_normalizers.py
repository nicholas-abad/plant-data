"""Tests for plant-name normalization — the crosswalk's matching keys.

The historical bug: suffix lists were applied with `str.replace` ANYWHERE
in the string, so " EXT" turned "WEST EXTENSION" into "WESTENSION" and
" IMP" turned "PUNTA IMPERIAL" into "PUNTA ERIAL" — and those mangled
forms were the fuzzy-match keys on BOTH sides.
"""

from src.plant_name_matchers.normalizers import (
    extract_base_name,
    normalize_for_comparison,
    normalize_gppd_name,
    validate_match,
)


class TestAnchoredSuffixStripping:
    def test_mid_name_fragments_are_preserved(self):
        # " EXT" must only strip at the END, not inside "EXTENSION"
        assert "EXTENSION" in normalize_for_comparison("WEST EXTENSION")
        # " IMP" must not eat the middle of "IMPERIAL"
        assert "IMPERIAL" in normalize_for_comparison("PUNTA IMPERIAL")
        # " GT" must not eat city names containing " GT" mid-string
        assert normalize_for_comparison("ANPARA D TPS") == "ANPARA D"

    def test_true_suffix_is_stripped(self):
        assert normalize_for_comparison("KORBA STPS") == "KORBA"
        assert extract_base_name("SINGRAULI STPS") == "SINGRAULI"

    def test_stacked_suffixes_fully_strip(self):
        # iterative stripping: "X TPS EXT" → "X TPS" → "X"
        assert normalize_for_comparison("BOKARO TPS EXT") == "BOKARO"

    def test_gem_style_suffixes(self):
        assert normalize_for_comparison("Foo power station") == "FOO"
        assert normalize_for_comparison("Foo power plant") == "FOO"

    def test_parentheticals_removed(self):
        assert normalize_for_comparison("BARAUNI TPS (Liq.)") == "BARAUNI"


class TestValidateMatch:
    def test_shared_location_word_passes(self):
        assert validate_match("KORBA STPS", "Korba power station") is True

    def test_documented_false_positive_is_rejected(self):
        # The code's own comment documents this fuzzy false positive:
        # "BHADRA HPS" must NOT validate against "Bhandara power station"
        assert validate_match("BHADRA HPS", "Bhandara power station") is False

    def test_stop_words_do_not_validate(self):
        # Sharing only generic words like "power"/"thermal" is not a match
        assert validate_match("THERMAL POWER LTD", "Other Thermal Power") is False


class TestGppdNormalize:
    def test_nan_returns_empty(self):
        import numpy as np

        assert normalize_gppd_name(np.nan) == ""

    def test_anchored_block_suffix(self):
        assert normalize_gppd_name("TUTICORIN BLOCK A") == "TUTICORIN"
        # but a name containing 'block' mid-string is untouched
        assert "BLOCKHAUS" in normalize_gppd_name("BLOCKHAUS STATION")


class TestValidateCoordinates:
    def test_null_island_rejected(self):
        from src.utils import validate_coordinates

        assert validate_coordinates(0, 0) is False
        assert validate_coordinates(0.0, 0.0) is False

    def test_real_coordinates_pass(self):
        from src.utils import validate_coordinates

        assert validate_coordinates(52.5, 13.4) is True
        assert validate_coordinates(-33.8, 151.2) is True

    def test_out_of_range_rejected(self):
        from src.utils import validate_coordinates

        assert validate_coordinates(91, 0) is False
        assert validate_coordinates(10, 181) is False

    def test_nan_rejected(self):
        import numpy as np

        from src.utils import validate_coordinates

        assert validate_coordinates(np.nan, 13.4) is False


class TestMojibakeAndAccents:
    def test_mojibake_repair(self):
        from src.plant_name_matchers.normalizers import fix_mojibake

        assert fix_mojibake("BeÅ\x82chatÃ³w B01") == "Bełchatów B01"
        assert fix_mojibake("plain ascii") == "plain ascii"
        assert fix_mojibake("café") == "café", "genuine latin-1 text untouched"

    def test_mojibake_names_key_identically(self):
        # The 13 Bełchatów units arrive double-encoded from ENTSO-E; they
        # must produce the SAME comparison key as the clean reference name
        # (ł doesn't NFKD-decompose, so both sides scrub it identically).
        key_mojibake = normalize_for_comparison("BeÅ\x82chatÃ³w")
        key_clean = normalize_for_comparison("Bełchatów")
        assert key_mojibake == key_clean
        assert validate_match("BeÅ\x82chatÃ³w B01", "Bełchatów") is True

    def test_accent_variant_validates(self):
        assert validate_match("Tucunare", "Tucunaré power station") is True

    def test_short_exact_name_validates(self):
        # All base words are short or stopwords → whole-name phrase rule
        assert validate_match("CSP", "CSP power station") is True
        assert validate_match("GNA I", "GNA I power station") is True
        assert validate_match("Sol", "Sol") is True
        # ...but a DIFFERENT short name still fails
        assert validate_match("Altos", "Atos power station") is False

    def test_underscore_coded_names_validate(self):
        assert validate_match("TE_STANARI", "Stanari Thermal Power Plant") is True
        assert validate_match("TPP_BOBOV_DOL", "Bobov Dol power station") is True


class TestSuffixOrderingAndRetriever:
    def test_thermal_power_station_fully_strips(self):
        # Regression: " POWER STATION" must not strip before " THERMAL POWER
        # STATION" and leave a "FOO THERMAL" residue that can't match "FOO".
        assert normalize_for_comparison("Foo Thermal Power Station") == "FOO"
        assert (
            normalize_for_comparison("Vindhyachal Thermal Power Station")
            == "VINDHYACHAL"
        )

    def test_build_norm_index_first_wins_and_drops_empty(self):
        from src.plant_name_matchers.normalizers import build_norm_index

        idx = build_norm_index(
            ["Foo power station", "Foo power plant", "(Liq.)", "Korba power station"],
            normalize_for_comparison,
            "test",
        )
        assert "" not in idx, "empty-normalizing names excluded"
        assert idx["FOO"] == "Foo power station", "first-wins, deterministic"
        assert idx["KORBA"] == "Korba power station"

    def test_retriever_uses_shared_index(self):
        from src.plant_name_matchers.retriever import CandidateRetriever

        r = CandidateRetriever(
            {"GEM": ["Foo power station", "Foo power plant", "(Liq.)"]}
        )
        norm = r._normalized["GEM"]
        assert "" not in norm
        assert norm["FOO"] == "Foo power station"

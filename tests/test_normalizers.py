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

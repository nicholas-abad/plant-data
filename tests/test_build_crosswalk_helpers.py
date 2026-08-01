"""Tests for the crosswalk builder's pure matching helpers."""

import pandas as pd

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


class TestHjksOcctoCapacity:
    def _write_csv(self, tmp_path):
        p = tmp_path / "hjks_units.csv"
        p.write_text(
            "エリア,発電事業者,発電所コード,発電所名,発電形式,ユニット名,"
            "認可出力,認可出力（変更後）,適用開始日,稼働開始日,稼働終了日,最終更新日時\n"
            # Hofu: one active coal unit, ends in the future
            '"中国","X","72203","防府バイオマス発電所","火力（石炭）","1号機",'
            '"112000","","","2019/07/21","2039/07/20","2023/01/17 15:54",\n'
            # UBE registration A
            '"中国","Y","5AWR771132","宇部発電所","火力（石炭）","6号",'
            '"216000","","","2004/03/01","9999/12/31","2026/03/26 16:43",\n'
            # UBE registration B — a recorded modification wins over original
            '"中国","Z","6038771106","宇部興産発電所","火力（石炭）","5号機",'
            '"140000","145000","","1900/01/01","9999/12/31","2022/08/02 13:36",\n'
            # Retired unit: must NOT count
            '"東北","W","99999","終了発電所","火力（石炭）","1号機",'
            '"500000","","","1990/01/01","2020/03/31","2020/04/01 00:00",\n'
            # Multi-fuel site: gas + oil units share the code with a coal unit;
            # only the coal unit may count (the dashboard numerator is coal-only)
            '"北陸","V","88888","富山新港火力発電所","火力（ガス）","1号機",'
            '"424700","","","1900/01/01","9999/12/31","2020/01/01 00:00",\n'
            '"北陸","V","88888","富山新港火力発電所","火力（石油）","2号機",'
            '"240000","","","1900/01/01","9999/12/31","2020/01/01 00:00",\n'
            '"北陸","V","88888","富山新港火力発電所","火力（石炭）","石炭1号機",'
            '"250000","","","1900/01/01","9999/12/31","2020/01/01 00:00",\n'
            # Same physical unit registered under two grid-area codes: summing
            # a plant's codes must not count it twice
            '"東北","U","70001","勿来発電所","火力（石炭）","8号機",'
            '"600000","","","1900/01/01","9999/12/31","2020/01/01 00:00",\n'
            '"東京","U","70002","勿来発電所","火力（石炭）","8号機",'
            '"600000","","","1900/01/01","9999/12/31","2020/01/01 00:00",\n',
            encoding="utf-8",
        )
        return p

    def test_load_hjks_coal_units(self, tmp_path):
        from src.build_crosswalk import load_hjks_coal_units

        units = load_hjks_coal_units(self._write_csv(tmp_path))
        by_code = units.groupby("code")["mw"].sum().to_dict()
        assert by_code["72203"] == 112.0
        assert by_code["5AWR771132"] == 216.0
        assert by_code["6038771106"] == 145.0  # modified output wins
        assert "99999" not in by_code  # retired excluded
        # Coal filter: the multi-fuel site's gas (424.7) and oil (240) units
        # are absent — only its coal unit remains
        assert by_code["88888"] == 250.0

    def test_load_missing_file_raises(self, tmp_path):
        import pytest

        from src.build_crosswalk import load_hjks_coal_units

        with pytest.raises(FileNotFoundError):
            load_hjks_coal_units(tmp_path / "nope.csv")

    def test_apply_sums_codes_and_flags_source(self, tmp_path):
        from src.build_crosswalk import _apply_hjks_occto_capacity

        rows = pd.DataFrame(
            {
                "plant_name": ["防府バイオマス発電所", "宇部興産発電所", "usa plant"],
                "source_system": ["OCCTO", "OCCTO", "EIA"],
                "capacity_mw": [36.0, 145.0, 500.0],
            }
        )
        codes = {
            "防府バイオマス発電所": ["72203"],
            "宇部興産発電所": ["5AWR771132", "6038771106"],
        }
        out = _apply_hjks_occto_capacity(rows, codes, self._write_csv(tmp_path))
        by = dict(zip(out["plant_name"], out["capacity_mw"]))
        assert by["防府バイオマス発電所"] == 112.0
        assert by["宇部興産発電所"] == 216.0 + 145.0
        assert by["usa plant"] == 500.0  # non-OCCTO untouched
        src_by = dict(zip(out["plant_name"], out["capacity_source"]))
        assert src_by["防府バイオマス発電所"] == "HJKS"
        assert pd.isna(src_by["usa plant"]) or src_by["usa plant"] is None

    def test_partial_code_coverage_keeps_existing_value(self, tmp_path):
        # A partial sum would silently overwrite GEM with a too-small
        # denominator — the inverse of the bug HJKS fixes. Any missing code
        # means: keep the existing capacity, no override.
        from src.build_crosswalk import _apply_hjks_occto_capacity

        rows = pd.DataFrame(
            {
                "plant_name": ["宇部興産発電所"],
                "source_system": ["OCCTO"],
                "capacity_mw": [145.0],
            }
        )
        codes = {"宇部興産発電所": ["5AWR771132", "not-in-hjks"]}
        out = _apply_hjks_occto_capacity(rows, codes, self._write_csv(tmp_path))
        assert out["capacity_mw"].item() == 145.0
        assert out["capacity_source"].isna().all()

    def test_duplicate_registrations_counted_once(self, tmp_path):
        # 勿来 8号機 is registered per grid area (two codes, same unit name):
        # summing the plant's codes must count the 600 MW hardware once.
        from src.build_crosswalk import _apply_hjks_occto_capacity

        rows = pd.DataFrame(
            {
                "plant_name": ["勿来発電所"],
                "source_system": ["OCCTO"],
                "capacity_mw": [1975.0],
            }
        )
        codes = {"勿来発電所": ["70001", "70002"]}
        out = _apply_hjks_occto_capacity(rows, codes, self._write_csv(tmp_path))
        assert out["capacity_mw"].item() == 600.0

"""npp_unit_gem_map: India's GEM identity at unit grain (no database)."""

import pandas as pd
import pytest

from src import build_crosswalk as bc
from src import gem_reference as gemref


@pytest.fixture
def gem(monkeypatch, tmp_path):
    locs = pd.DataFrame(
        {
            "gem_location_id": ["LB1", "LB2", "LK"],
            "name": [
                "Barh I power station",
                "Barh II power station",
                "Kawai power station",
            ],
            "country": ["India"] * 3,
        }
    ).set_index("gem_location_id", drop=False)
    units = pd.DataFrame(
        {
            "gem_unit_id": ["G1", "G2", "G3", "G4"],
            "gem_location_id": ["LB1", "LB1", "LB2", "LK"],
            "tracker": ["GCPT"] * 4,
            "status": ["operating"] * 4,
            "capacity_mw": [660.0] * 4,
        }
    )
    units["is_coal"] = True
    units["is_operating"] = True
    monkeypatch.setattr(gemref, "_TABLES", {"locations": locs, "units": units})
    gipt = pd.DataFrame(
        {
            "Type": ["coal"] * 3,
            # extra space as in the real file; must still match the stored name
            "DGR plant name": ["BARH  STPS"] * 3,
            "DGR unit": ["Unit 1", "Unit 2", "Unit 4"],
            "GEM unit/phase ID": ["G1", "G2", "G3"],
        }
    )
    path = tmp_path / "gipt.csv"
    gipt.to_csv(path, index=False)
    monkeypatch.setattr(bc, "NPP_GIPT_CSV", path)
    reviewed = tmp_path / "reviewed.csv"
    pd.DataFrame(columns=["plant", "unit", "gem_location_id", "gem_unit_ids", "evidence"]).to_csv(reviewed, index=False)
    monkeypatch.setattr(bc, "NPP_REVIEWED_CSV", reviewed)
    yield
    gemref._TABLES = None


def _crosswalk():
    return pd.DataFrame(
        {
            "plant_name": ["BARH STPS", "KAWAI TPS", "NEW TPS"],
            "source_system": ["NPP"] * 3,
            "gem_location_id": [None, "LK", None],
            "not_in_gem": [False, False, False],
        }
    )


def test_split_plant_units_land_on_their_own_sites(gem):
    units = pd.DataFrame(
        {"plant": ["BARH STPS"] * 3, "unit": ["Unit 1", "Unit 2", "Unit 4"]}
    )
    out = bc.build_npp_unit_map(units, _crosswalk()).set_index("unit")
    assert out.loc["Unit 1", "gem_location_id"] == "LB1"
    assert out.loc["Unit 4", "gem_location_id"] == "LB2"
    assert (out["map_source"] == "gipt-unit").all()


def test_units_without_gipt_inherit_the_plant_link(gem):
    units = pd.DataFrame({"plant": ["KAWAI TPS", "KAWAI TPS"], "unit": ["Unit 1", ""]})
    out = bc.build_npp_unit_map(units, _crosswalk())
    assert set(out["gem_location_id"]) == {"LK"}
    assert set(out["map_source"]) == {"crosswalk-plant"}


def test_unlinked_plant_without_gipt_is_left_out(gem):
    units = pd.DataFrame({"plant": ["NEW TPS"], "unit": ["Unit 1"]})
    assert bc.build_npp_unit_map(units, _crosswalk()).empty


def test_not_in_gem_plant_is_never_placed(gem):
    # A person decided this NPP row has no GEM plant of its own (e.g. the
    # grid-export meter of a captive plant): GIPT must not place its units.
    units = pd.DataFrame(
        {"plant": ["BARH STPS", "BARH STPS", "KAWAI TPS"], "unit": ["Unit 1", "Unit 4", "Unit 1"]}
    )
    cw = _crosswalk()
    out = bc.build_npp_unit_map(units, cw)
    assert set(out["plant"]) == {"BARH STPS", "KAWAI TPS"}
    cw.loc[cw["plant_name"] == "BARH STPS", "not_in_gem"] = True
    out = bc.build_npp_unit_map(units, cw)
    assert list(out["plant"]) == ["KAWAI TPS"]
    assert list(out["map_source"]) == ["crosswalk-plant"] and list(out["gem_location_id"]) == ["LK"]


def test_only_the_not_in_gem_plant_loses_its_gipt_units(gem):
    # Both plants are in the GIPT file; only the marked one may drop out.
    gipt = pd.read_csv(bc.NPP_GIPT_CSV)
    gipt.loc[len(gipt)] = ["coal", "NEW TPS", "Unit 1", "G4"]
    gipt.to_csv(bc.NPP_GIPT_CSV, index=False)
    units = pd.DataFrame({"plant": ["BARH STPS", "NEW TPS"], "unit": ["Unit 1", "Unit 1"]})
    cw = _crosswalk()
    cw.loc[cw["plant_name"] == "BARH STPS", "not_in_gem"] = True
    out = bc.build_npp_unit_map(units, cw)
    assert list(out["plant"]) == ["NEW TPS"]
    assert list(out["map_source"]) == ["gipt-unit"] and list(out["gem_location_id"]) == ["LK"]


def test_gipt_unit_on_two_sites_fails_loudly(gem, tmp_path, monkeypatch):
    gipt = pd.read_csv(bc.NPP_GIPT_CSV)
    gipt.loc[len(gipt)] = ["coal", "BARH  STPS", "Unit 1", "G4"]
    gipt.to_csv(bc.NPP_GIPT_CSV, index=False)
    units = pd.DataFrame({"plant": ["BARH STPS"], "unit": ["Unit 1"]})
    with pytest.raises(ValueError, match="more than one GEM location"):
        bc.build_npp_unit_map(units, _crosswalk())


def test_npp_rows_take_the_shared_unit_map_shape():
    from src.unit_gem_map import COLUMNS

    npp_map = pd.DataFrame(
        [("BARH STPS", "Unit 4", "LB2", "G3", "gipt-unit"), ("KAWAI", "Unit 1", "LK", None, "crosswalk-plant")],
        columns=["plant", "unit", "gem_location_id", "gem_unit_id", "map_source"],
    )
    units = pd.DataFrame(
        [("BARH STPS", "Unit 4", "THERMAL"), ("KAWAI", "Unit 1", "HYDRO")], columns=["plant", "unit", "fuel_type"]
    )
    rows = bc.npp_unit_gem_rows(npp_map, units)
    assert list(rows.columns) == COLUMNS and set(rows["source_system"]) == {"NPP"}
    barh = rows.iloc[0]
    assert (barh["plant_key"], barh["unit_key"], barh["gem_unit_ids"], barh["is_coal"]) == ("BARH STPS", "Unit 4", "G3", True)
    assert rows.iloc[1]["fuel"] == "HYDRO" and not rows.iloc[1]["is_coal"] and pd.isna(rows.iloc[1]["gem_unit_ids"])


def _review(rows):
    path = bc.NPP_REVIEWED_CSV
    pd.DataFrame(rows, columns=["plant", "unit", "gem_location_id", "gem_unit_ids", "evidence"]).to_csv(path, index=False)


def test_a_reviewed_unit_replaces_gipt_and_places_a_renamed_plant(gem):
    # GIPT puts BARH Unit 4 on G3; the review says G4 at Kawai. A plant DGR
    # renamed after GIPT was made (KAWAI NEW) is placed only by the review.
    _review([("BARH STPS", "Unit 4", "LK", "G4", "x"), ("KAWAI NEW", "Unit 1", "LK", "G4", "renamed")])
    units = pd.DataFrame({"plant": ["BARH STPS", "BARH STPS", "KAWAI NEW"], "unit": ["Unit 1", "Unit 4", "Unit 1"]})
    out = bc.build_npp_unit_map(units, _crosswalk()).set_index(["plant", "unit"])
    assert out.loc[("BARH STPS", "Unit 4"), "gem_unit_id"] == "G4"
    assert out.loc[("BARH STPS", "Unit 4"), "map_source"] == "source-id-reviewed"
    assert out.loc[("BARH STPS", "Unit 1"), "map_source"] == "gipt-unit"
    assert out.loc[("KAWAI NEW", "Unit 1"), "gem_location_id"] == "LK"


def test_a_reviewed_unit_at_the_wrong_gem_plant_fails(gem):
    _review([("BARH STPS", "Unit 4", "LB1", "G4", "x")])
    units = pd.DataFrame({"plant": ["BARH STPS"], "unit": ["Unit 4"]})
    with pytest.raises(ValueError, match="G4"):
        bc.build_npp_unit_map(units, _crosswalk())

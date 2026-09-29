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
    yield
    gemref._TABLES = None


def _crosswalk():
    return pd.DataFrame(
        {
            "plant_name": ["BARH STPS", "KAWAI TPS", "NEW TPS"],
            "source_system": ["NPP"] * 3,
            "gem_location_id": [None, "LK", None],
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


def test_gipt_unit_on_two_sites_fails_loudly(gem, tmp_path, monkeypatch):
    gipt = pd.read_csv(bc.NPP_GIPT_CSV)
    gipt.loc[len(gipt)] = ["coal", "BARH  STPS", "Unit 1", "G4"]
    gipt.to_csv(bc.NPP_GIPT_CSV, index=False)
    units = pd.DataFrame({"plant": ["BARH STPS"], "unit": ["Unit 1"]})
    with pytest.raises(ValueError, match="more than one GEM location"):
        bc.build_npp_unit_map(units, _crosswalk())

"""Dan E Karn and the two Hibikinada plants on their GEM plants (tracker #49), from the committed files.

* Dan E Karn (EIA 1702): GEM lists the code at two locations; the coal
  generators 1A/2A are Karn Generating Plant's Units 1 and 2, the gas/oil
  units 3/4 sit at Dan E Karn power station.
* OCCTO's two Hibikinada plants were both on Hibikinada Thermal Power Station;
  the coal/biomass plant of Hibikinada Energy Park is GEM's Hibikinida Biomass
  power station (its HJKS code is that unit's FIT ID).
"""

import importlib.util
from pathlib import Path

import pandas as pd
import pytest

from src import eia_gem_ids as ids
from src import gem_reference as gemref
from src.eia_unit_map import apply_review, build_eia_unit_map, gem_candidates
from src.gem_external_ids import SNAPSHOT_FILE
from src.occto_unit_map import REVIEWED_FILE as OCCTO_REVIEWED
from src.occto_unit_map import build_occto_unit_map
from src.utils import get_crosswalk_dir

KARN_COAL, KARN_GAS = "L100000103981", "L100000401829"
HIBIKINADA_THERMAL, HIBIKINIDA_BIOMASS = "L100000104537", "L100000102934"
THERMAL, ENERGY_PARK = "響灘火力発電所", "ひびき灘石炭・バイオマス発電所"


def manifest_rows() -> dict[str, dict]:
    """'SOURCE|key' → its row, over every committed decision manifest."""
    rows = {}
    for path in sorted(ids.MIGRATIONS_DIR.glob("*.csv")):
        for r in pd.read_csv(path, dtype=str, keep_default_na=False).to_dict("records"):
            rows[f"{r['source_system']}|{r['key']}"] = r
    return rows


@pytest.fixture
def karn_gem(monkeypatch):
    names = {KARN_COAL: "Karn Generating Plant", KARN_GAS: "Dan E Karn power station"}
    monkeypatch.setattr(
        gemref, "location", lambda loc: {"name": names[loc]} if loc in names else None
    )


def test_karn_links_to_gems_coal_location(karn_gem):
    review = ids.load_review()
    assert ids.target("1702", {KARN_COAL, KARN_GAS}, "MI", review) == ids.Target(
        KARN_COAL, "review:accept"
    )
    assert review["1702"]["reviewed_by"]
    # The row is pinned by the cutover's legacy decision (unlinked): tier 0 keeps
    # that unless a manifest replaces it with the reviewed location.
    m = manifest_rows()["EIA|1702"]
    assert (m["old_gem_location_id"], m["old_decided_by"]) == ("", "legacy-pipeline")
    assert m["gem_location_id"] == review["1702"]["gem_location_id"] == KARN_COAL


def test_review_file_keeps_karn_once_the_manifest_is_applied(karn_gem):
    """After the manifest the live link IS the accepted one; regenerating the
    review file must keep the reviewed row, which the unit map still reads."""
    path = Path(__file__).resolve().parents[1] / "scripts" / "eia_gem_id_review.py"
    spec = importlib.util.spec_from_file_location("eia_gem_id_review", path)
    review_script = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(review_script)
    old = ids.load_review()
    live = pd.DataFrame(
        [("1702", "Dan E Karn", KARN_COAL, "manual", old["1702"]["reviewed_by"], False, "MI")],
        columns=["plant_code", "plant_name", "gem_location_id", "matching_method",
                 "decided_by", "not_in_gem", "eia_state"],
    )  # fmt: skip
    out = review_script.build_review(live, {"1702": {KARN_COAL, KARN_GAS}}, old)
    (row,) = out.to_dict("records")
    assert (row["verdict"], row["gem_location_id"]) == ("accept", KARN_COAL)
    assert row["reviewed_by"] == old["1702"]["reviewed_by"]


def test_karn_coal_generators_land_on_gems_coal_units():
    snapshot = pd.read_parquet(SNAPSHOT_FILE)
    cands = gem_candidates(snapshot)
    cands = apply_review(cands[cands["plant_code"] == "1702"], ids.load_review())
    cands = cands.rename(columns={"gem_generator_id": "generator_id"})
    generators = pd.DataFrame(
        [("1702", "1A", "Conventional Steam Coal"), ("1702", "2A", "Conventional Steam Coal"),
         ("1702", "3", "Natural Gas Steam Turbine"), ("1702", "4", "Natural Gas Steam Turbine")],
        columns=["plant_code", "generator_id", "technology"],
    )  # fmt: skip
    units = pd.DataFrame(
        [("G100000105598", "GCPT", "retired"), ("G100000105599", "GCPT", "retired"),
         ("G100000402642", "GOGPT", "operating"), ("G100000402643", "GOGPT", "operating")],
        columns=["gem_unit_id", "tracker", "status"],
    )  # fmt: skip
    out, _ = build_eia_unit_map(generators, cands, units)
    got = dict(zip(out["unit_key"], out["gem_unit_id"]))
    # Only the accepted location places units; 3 and 4 fall back to the plant link.
    assert got == {"1A": "G100000105598", "2A": "G100000105599"}
    assert out["is_coal"].all() and set(out["gem_location_id"]) == {KARN_COAL}


def test_energy_park_hjks_code_is_gems_fit_id():
    hjks = pd.read_csv(
        get_crosswalk_dir() / "hjks_units.csv", dtype=str, index_col=False
    )  # rows end in a comma
    (code,) = hjks.loc[hjks["発電所名"] == ENERGY_PARK, "発電所コード"]
    snapshot = pd.read_parquet(SNAPSHOT_FILE)
    fit = snapshot[(snapshot["namespace"] == "FIT") & (snapshot["external_id"] == code)]
    assert set(zip(fit["gem_location_id"], fit["gem_unit_id"])) == {
        (HIBIKINIDA_BIOMASS, "G100000103883")
    }


def test_hibikinada_plants_split_onto_their_own_gem_plants():
    m = manifest_rows()
    row = m[f"OCCTO|{ENERGY_PARK}"]
    assert (row["old_gem_location_id"], row["old_decided_by"]) == (
        HIBIKINADA_THERMAL,
        "legacy-pipeline",
    )
    assert row["gem_location_id"] == HIBIKINIDA_BIOMASS
    assert (
        f"OCCTO|{THERMAL}" not in m
    )  # 響灘火力発電所 is Hibikinada Thermal Power Station: unchanged

    links = {THERMAL: HIBIKINADA_THERMAL, ENERGY_PARK: row["gem_location_id"]}
    units = pd.DataFrame(
        [(ENERGY_PARK, "単独", "coal"), (THERMAL, "1号機", "coal")],
        columns=["plant", "unit", "fuel_type"],
    )
    gem = pd.DataFrame(
        [("G100000103883", HIBIKINIDA_BIOMASS, "GCPT", "--", "operating"),
         ("G100000113920", HIBIKINADA_THERMAL, "GCPT", "1, timepoint 1", "mothballed"),
         ("G100000202486", HIBIKINADA_THERMAL, "GBPT", "1, timepoint 2", "operating")],
        columns=["gem_unit_id", "gem_location_id", "tracker", "unit_name", "status"],
    )  # fmt: skip
    reviewed = pd.read_csv(OCCTO_REVIEWED, dtype=str, keep_default_na=False)
    reviewed = reviewed[reviewed["plant"].isin(links)]
    out = build_occto_unit_map(units, reviewed, gem, links).set_index("plant_key")
    assert out.loc[ENERGY_PARK, ["gem_location_id", "gem_unit_id"]].tolist() == [
        HIBIKINIDA_BIOMASS,
        "G100000103883",
    ]
    assert out.loc[THERMAL, ["gem_location_id", "gem_unit_id"]].tolist() == [
        HIBIKINADA_THERMAL,
        "G100000113920",
    ]
    assert set(out["map_source"]) == {"source-id-reviewed"}

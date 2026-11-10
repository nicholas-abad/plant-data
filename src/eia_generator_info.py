"""EIA Form 860 generator reference rows for the `eia_generator_info` table.

Every coal reader identifies coal generation by joining generator-level
generation to this table on (plant_code, generator_id) and filtering on
`technology`. Form 860's "Operable" sheet lists only generators still
operating at the end of the file's year, so a coal unit retired earlier drops
out of the join and its history silently stops counting as coal.

So the table also carries RETIRED generators (status 'RE') from the "Retired
and Canceled" sheet. One edition's retired sheet is not complete: the 2024
sheet has Dan E Karn (retired 2023) but not W H Zimmer (2022) or W H Sammis
(2023), which left 46 coal units and 93.6 TWh of 2019-2023 generation without
a technology. The table is therefore:

* the base edition (2024): its Operable sheet plus its retired 'RE' rows;
* plus, for generators the base edition does not have, the 'RE' row of the
  newest older edition (2019-2023) that lists them. Those rows are committed
  as `data/crosswalks/eia860_retired_2019_2023.csv`, built by
  `scripts/build_eia860_supplement.py`; the workbooks are not committed.

A base-edition row is never replaced, so a generator Operable in 2024 keeps
its status. Canceled ('CN') and indefinitely postponed ('IP') generators never
ran and are left out. `status` is Form 860's own code, so a reader summing
nameplate for current capacity can exclude 'RE'. `source_edition` is the
Form 860 year a row comes from.

"The same generator" is decided on `eia_generator_key`, the Python mirror of
the database function the loader keys the table with (ETL migration 023), not
on the raw ID: Form 860 spells one generator "1" in one place and "01" in
another.
"""

from __future__ import annotations

from pathlib import Path
from typing import NamedTuple

import pandas as pd

from .utils import get_crosswalk_dir

OPERABLE_SHEET = "Operable"
RETIRED_SHEET = "Retired and Canceled"
RETIRED_STATUS = "RE"
# The tracker's coal technologies (EIA_COAL_TECHNOLOGIES) and the coal review query's.
COAL_TECHNOLOGIES = {"Conventional Steam Coal", "Coal Integrated Gasification Combined Cycle", "Petroleum Coke"}

BASE_EDITION = 2024
SUPPLEMENT_EDITIONS = (2019, 2020, 2021, 2022, 2023)
BASE_WORKBOOK = get_crosswalk_dir() / f"3_1_Generator_Y{BASE_EDITION}.xlsx"
SUPPLEMENT_FILE = get_crosswalk_dir() / "eia860_retired_2019_2023.csv"
SUPPLEMENT_MANIFEST = SUPPLEMENT_FILE.with_suffix(".manifest.json")

_COLUMNS = {
    "Plant Code": "plant_code",
    "Generator ID": "generator_id",
    "Technology": "technology",
    "Prime Mover": "prime_mover",
    "Energy Source 1": "energy_source_1",
    "Nameplate Capacity (MW)": "nameplate_capacity_mw",
    "Status": "status",
}
TABLE_COLUMNS = [*_COLUMNS.values(), "source_edition"]
_KEY = ["plant_code", "generator_id"]


def eia_generator_key(plant_code: str, generator_id: str) -> str:
    """One spelling per generator ID, exactly as public.eia_generator_key() in the database.

    Capitals, no spaces, no leading zeros ("01A" → "1A", "000" → "0"). Two
    written exceptions, as in ETL migration 023: Rockport (6166) reports
    MB1/MB2 for its units 1/2, and plant 56032 keeps its IDs because Form 860
    lists "0001" and "001" there as two turbines.
    tests/test_eia_generator_key_sql.py holds the two to each other.
    """
    # Only the ASCII space, like the SQL's replace(generator_id, ' ', '').
    compact = str(generator_id).upper().replace(" ", "")
    if plant_code == "6166" and compact in ("MB1", "MB2"):
        return compact[-1]
    if plant_code == "56032":
        return compact
    return compact.lstrip("0") or "0"


def _with_key(df: pd.DataFrame) -> pd.Series:
    return pd.Series(
        [eia_generator_key(p, g) for p, g in zip(df["plant_code"], df["generator_id"])],
        index=df.index,
        dtype=object,
    )


def _sheet(path: Path, sheet: str) -> pd.DataFrame:
    # IDs are text: left to pandas, a sheet whose IDs all look numeric would
    # read "0001" as 1 and lose what tells two generators apart.
    df = pd.read_excel(
        path, sheet_name=sheet, skiprows=1, usecols=list(_COLUMNS), dtype={"Generator ID": str}
    )
    df = df.rename(columns=_COLUMNS)
    # Trailing footnote rows in the xlsx carry no keys.
    df = df.dropna(subset=_KEY).copy()
    # Join-key types must match the generation tables (VARCHAR).
    df["plant_code"] = df["plant_code"].astype(int).astype(str)
    df["generator_id"] = df["generator_id"].astype(str)
    return df[list(_COLUMNS.values())]


def _fail_on_shared_keys(df: pd.DataFrame, what: str) -> None:
    """The table's primary key is (plant_code, keyed generator_id).

    A silent pick between two rows would decide whether a unit counts as coal,
    and two spellings of one generator would fail the loader's swap in SQL.
    """
    key = _with_key(df)
    dupes = pd.DataFrame({"plant_code": df["plant_code"], "key": key}).duplicated(keep=False)
    if dupes.any():
        raise ValueError(
            f"{what}: {int(dupes.sum())} generator rows share a (plant_code, generator_id), e.g. "
            f"{df.loc[dupes, _KEY].head(4).values.tolist()}"
        )


def retired_rows(path: Path, edition: int) -> pd.DataFrame:
    """One edition's retired ('RE') generators, with the edition they come from."""
    retired = _sheet(path, RETIRED_SHEET)
    retired = retired[retired["status"] == RETIRED_STATUS].assign(source_edition=edition)
    _fail_on_shared_keys(retired, f"Form 860 {edition} retired sheet")
    return retired[TABLE_COLUMNS].reset_index(drop=True)


def base_frame(path: Path, edition: int = BASE_EDITION) -> pd.DataFrame:
    """The base edition: its Operable generators plus its retired ones."""
    operable = _sheet(path, OPERABLE_SHEET).assign(source_edition=edition)
    df = pd.concat([operable, retired_rows(path, edition)], ignore_index=True)
    _fail_on_shared_keys(df, f"Form 860 {edition}")
    return df[TABLE_COLUMNS]


class RetiredSupplement(NamedTuple):
    """What the older editions add to the base edition, and what they disagree on."""

    # One 'RE' row per generator the base edition lacks, from the newest edition listing it.
    rows: pd.DataFrame
    # Generators whose technology differs between editions without changing whether they
    # are coal among the added rows, or in any way against the base edition (which wins):
    # plant_code, generator_id, technologies ("edition: technology; …"), kept.
    technology_changes: pd.DataFrame


def _technologies(group: pd.DataFrame) -> str:
    return "; ".join(f"{e}: {t}" for e, t in zip(group["source_edition"], group["technology"].fillna("(none)")))


def retired_supplement(older: dict[int, pd.DataFrame], base: pd.DataFrame) -> RetiredSupplement:
    """The retired generators of ``older`` editions that ``base`` does not have.

    ``older`` maps an edition year to its ``retired_rows``. For a generator in
    several editions the newest one wins. A generator already in ``base``
    (Operable or retired) is never added.

    Raises ValueError when a generator this adds is a coal technology in one
    edition and something else in another: which edition is right decides
    whether its history counts as coal, and that needs a person.
    """
    every = pd.concat([older[e] for e in sorted(older)], ignore_index=True)
    every = every.assign(_key=_with_key(every))
    base_technology = dict(zip(zip(base["plant_code"], _with_key(base)), base["technology"]))
    in_base = pd.Series(
        [(p, k) in base_technology for p, k in zip(every["plant_code"], every["_key"])], index=every.index
    )
    added = every[~in_base]

    changes, coal_conflicts = [], []
    for (plant, _), group in added.groupby(["plant_code", "_key"], sort=True):
        technologies = set(group["technology"].fillna("(none)"))
        if len(technologies) == 1:
            continue
        newest = group.iloc[-1]
        if 0 < len(technologies & COAL_TECHNOLOGIES) < len(technologies):
            coal_conflicts.append(f"{plant}/{newest['generator_id']} ({_technologies(group)})")
        changes.append((plant, newest["generator_id"], _technologies(group), f"{newest['source_edition']} (newest)"))
    if coal_conflicts:
        raise ValueError(
            f"{len(coal_conflicts)} retired generators are coal in one Form 860 edition and not in another; "
            f"decide which is right before loading: {'; '.join(coal_conflicts[:10])}"
        )
    for (plant, key), group in every[in_base].groupby(["plant_code", "_key"], sort=True):
        kept = base_technology[(plant, key)]
        differing = group[group["technology"].fillna("(none)") != (kept if isinstance(kept, str) else "(none)")]
        if len(differing):
            changes.append(
                (plant, differing.iloc[-1]["generator_id"], _technologies(differing), f"base edition: {kept}")
            )

    rows = added.drop_duplicates(["plant_code", "_key"], keep="last")  # sorted by edition: newest last
    rows = rows.sort_values(["plant_code", "_key"], key=lambda s: s.astype(int) if s.name == "plant_code" else s)
    return RetiredSupplement(
        rows=rows[TABLE_COLUMNS].reset_index(drop=True),
        technology_changes=pd.DataFrame(changes, columns=["plant_code", "generator_id", "technologies", "kept"]),
    )


def read_supplement(path: Path) -> pd.DataFrame:
    """The committed supplement, typed like ``base_frame`` (IDs stay text: "NA", "01")."""
    df = pd.read_csv(
        path,
        dtype={"plant_code": str, "generator_id": str, "source_edition": int, "nameplate_capacity_mw": float},
        keep_default_na=False,
        na_values={c: [""] for c in ("technology", "prime_mover", "energy_source_1", "nameplate_capacity_mw")},
    )
    missing = [c for c in TABLE_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"{path.name}: missing columns {missing}")
    not_retired = df[df["status"] != RETIRED_STATUS]
    if len(not_retired):
        raise ValueError(
            f"{path.name}: {len(not_retired)} rows are not status '{RETIRED_STATUS}'; a row from an older "
            "edition with an operating status would count as current capacity"
        )
    return df[TABLE_COLUMNS]


def eia_generator_info_frame(
    path: Path, supplement: Path | None = None, edition: int = BASE_EDITION
) -> pd.DataFrame:
    """The base edition's generators plus the retired ones older editions add.

    ``path`` is the base edition's workbook, ``supplement`` the committed CSV
    of older retired generators (None: the base edition alone). A supplement
    row whose generator the base edition has is dropped, so a newer base
    edition can never produce a second row for a generator, and an Operable
    generator is never overridden.

    Raises ValueError if two rows are one generator (by ``eia_generator_key``).
    """
    df = base_frame(path, edition)
    if supplement is not None:
        older = read_supplement(supplement)
        base_keys = set(zip(df["plant_code"], _with_key(df)))
        absent = [(p, k) not in base_keys for p, k in zip(older["plant_code"], _with_key(older))]
        df = pd.concat([df, older[absent]], ignore_index=True)
        _fail_on_shared_keys(df, f"Form 860 {edition} with {supplement.name}")
    return df[TABLE_COLUMNS].reset_index(drop=True)

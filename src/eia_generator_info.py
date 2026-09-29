"""EIA Form 860 generator reference rows for the `eia_generator_info` table.

Every coal reader identifies coal generation by joining generator-level
generation to this table on (plant_code, generator_id) and filtering on
`technology`. Form 860's "Operable" sheet lists only generators still
operating at the end of the file's year, so a coal unit retired earlier drops
out of the join and its history silently stops counting as coal: 12.3 TWh of
2023 and 2.3 TWh of 2024 US coal (Homer City, Pirkey, A.B. Brown, Sammis, …),
which left 2023 at 0.972 of EIA's published total.

So the table also carries the "Retired and Canceled" sheet's RETIRED
generators (status 'RE'). Canceled ('CN') and indefinitely postponed ('IP')
generators never ran and are left out. `status` is Form 860's own code, so a
reader summing nameplate for current capacity can exclude 'RE'.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

OPERABLE_SHEET = "Operable"
RETIRED_SHEET = "Retired and Canceled"
RETIRED_STATUS = "RE"

_COLUMNS = {
    "Plant Code": "plant_code",
    "Generator ID": "generator_id",
    "Technology": "technology",
    "Prime Mover": "prime_mover",
    "Energy Source 1": "energy_source_1",
    "Nameplate Capacity (MW)": "nameplate_capacity_mw",
    "Status": "status",
}


def _sheet(path: Path, sheet: str) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=sheet, skiprows=1, usecols=list(_COLUMNS))
    df = df.rename(columns=_COLUMNS)
    # Trailing footnote rows in the xlsx carry no keys.
    return df.dropna(subset=["plant_code", "generator_id"])


def eia_generator_info_frame(path: Path) -> pd.DataFrame:
    """Operable generators plus retired ones, keyed like the generation tables.

    Raises ValueError if a generator appears on both sheets or twice within
    one: the table's primary key is (plant_code, generator_id), and a silent
    pick between two rows would decide whether a unit counts as coal.
    """
    operable = _sheet(path, OPERABLE_SHEET)
    retired = _sheet(path, RETIRED_SHEET)
    retired = retired[retired["status"] == RETIRED_STATUS]
    df = pd.concat([operable, retired], ignore_index=True)
    # Join-key types must match the generation tables (VARCHAR).
    df["plant_code"] = df["plant_code"].astype(int).astype(str)
    df["generator_id"] = df["generator_id"].astype(str)
    dupes = df.duplicated(["plant_code", "generator_id"], keep=False)
    if dupes.any():
        raise ValueError(
            f"{int(dupes.sum())} generator rows share a (plant_code, generator_id), e.g. "
            f"{df.loc[dupes, ['plant_code', 'generator_id']].head(3).values.tolist()}"
        )
    return df[list(_COLUMNS.values())].reset_index(drop=True)

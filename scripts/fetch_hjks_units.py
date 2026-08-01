#!/usr/bin/env python3
"""Fetch the HJKS generation-unit list into data/crosswalks/hjks_units.csv.

HJKS (発電情報公開システム, hjks.jepx.or.jp) publishes Japan's disclosed
generation units with their AUTHORIZED RATED OUTPUT (認可出力, kW) keyed by
発電所コード — the same plant-code namespace occto_generation_data carries.
This is the authoritative capacity source for Japanese plants: GEM only
knows a plant's coal slice, while OCCTO meters whole plants, which made
co-fired/captive plants read >100% CF (Hofu Biomass 219%, UBE 122%).

Plain HTTP: GET the search page for a session cookie + CSRF token, then POST
the form with csv=csv (the CSVダウンロード button). No Selenium. Response is
CP932-encoded; re-encoded to UTF-8 on write.

Usage:
    uv run python scripts/fetch_hjks_units.py

The output CSV is committed (like the GEM CSV): build_crosswalk.py reads it
offline, and the commit history records when the reference was refreshed.
"""

import re
import ssl
import sys
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.utils import get_crosswalk_dir  # noqa: E402

HJKS_URL = "https://hjks.jepx.or.jp/hjks/unit"
OUTPUT = get_crosswalk_dir() / "hjks_units.csv"


class _LegacyTlsAdapter(HTTPAdapter):
    """hjks.jepx.or.jp only offers TLS1.2 with ECDHE-RSA-AES128-SHA — a SHA-1
    CBC suite that OpenSSL's default security level refuses. Lower the cipher
    security level for THIS host only; certificate verification stays on."""

    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.set_ciphers("DEFAULT:@SECLEVEL=1")
        kwargs["ssl_context"] = ctx
        return super().init_poolmanager(*args, **kwargs)

EXPECTED_COLUMNS = [
    "エリア", "発電事業者", "発電所コード", "発電所名", "発電形式",
    "ユニット名", "認可出力", "認可出力（変更後）", "適用開始日",
    "稼働開始日", "稼働終了日", "最終更新日時",
]


def main() -> int:
    session = requests.Session()
    session.mount("https://hjks.jepx.or.jp/", _LegacyTlsAdapter())
    page = session.get(HJKS_URL, timeout=60)
    page.raise_for_status()
    m = re.search(r'name="_csrf" value="([^"]+)"', page.text)
    if not m:
        raise RuntimeError("HJKS: no CSRF token on the search page — layout changed?")

    resp = session.post(
        HJKS_URL,
        data={
            "area": "", "company": "", "plantcd": "", "name": "",
            "format": "", "unitname": "",
            # enddtFlg=true means "hide decommissioned units" — we WANT ended
            # units too (their 稼働終了日 lets the consumer exclude them with
            # an explicit date rather than by absence).
            "_enddtFlg": "on",
            "csv": "csv",
            "_csrf": m.group(1),
        },
        timeout=120,
    )
    resp.raise_for_status()
    if "csv" not in resp.headers.get("Content-Type", ""):
        raise RuntimeError(
            f"HJKS: expected text/csv, got {resp.headers.get('Content-Type')!r} — "
            "form fields or CSRF handling changed?"
        )

    text = resp.content.decode("cp932")
    OUTPUT.write_text(text, encoding="utf-8")

    # index_col=False: every data row ends with a trailing comma (13 fields
    # vs 12 headers); without it pandas silently promotes the first column to
    # the index, shifting every value one column left.
    df = pd.read_csv(OUTPUT, index_col=False, dtype={"発電所コード": str})
    missing = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    if missing:
        raise RuntimeError(f"HJKS CSV missing expected columns: {missing}")
    if len(df) < 300:
        raise RuntimeError(
            f"HJKS CSV has only {len(df)} rows — a full pull returns 500+; "
            "did a filter apply?"
        )
    coal = df["発電形式"].astype(str).str.contains("石炭").sum()
    print(f"Wrote {OUTPUT}: {len(df)} units ({coal} coal)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

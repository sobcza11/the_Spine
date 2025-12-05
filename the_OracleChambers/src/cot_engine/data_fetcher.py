"""
Data fetcher for CFTC COT 'Other (Combined) - Long Report'.
"""

from __future__ import annotations

import requests

# CFTC "Other (Combined) - Futures & Options" long report
CFTC_URL_OTHER_LONG = "https://www.cftc.gov/dea/futures/other_lf.htm"


def fetch_cftc_data(debug_mode: bool = False) -> str | None:
    """
    Fetch raw HTML/text of the CFTC 'Other (Combined) - Long Report'.

    Returns:
        str: Raw response text on success.
        None: On failure.
    """
    try:
        resp = requests.get(CFTC_URL_OTHER_LONG, timeout=30)
        resp.raise_for_status()
        text = resp.text
        if debug_mode:
            print(f"[fetch_cftc_data] Downloaded {len(text)} chars from CFTC.")
        return text
    except Exception as e:
        if debug_mode:
            print("[fetch_cftc_data] Failed to fetch CFTC data:", e)
        return None


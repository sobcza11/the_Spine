from __future__ import annotations

from datetime import datetime, timezone
from io import StringIO
import os

import pandas as pd
import requests


FRED_API_KEY = os.environ["FRED_API_KEY"]
FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

# Governed FRED series
SERIES = {
    "target_upper": "DFEDTARU",  # Federal Funds Target Range - Upper Limit
    "target_lower": "DFEDTARL",  # Federal Funds Target Range - Lower Limit
    "effective_rate": "DFF",     # Effective Federal Funds Rate
}


def _fetch_fred_series(series_id: str) -> pd.DataFrame:
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": "desc",
        "limit": 30,
    }

    resp = requests.get(FRED_BASE_URL, params=params, timeout=30)
    resp.raise_for_status()

    payload = resp.json()
    observations = payload.get("observations", [])

    if not observations:
        raise ValueError(f"No observations returned for FRED series {series_id}")

    df = pd.DataFrame(observations)[["date", "value"]].copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["date", "value"]).sort_values("date").reset_index(drop=True)

    if df.empty:
        raise ValueError(f"No valid numeric observations found for FRED series {series_id}")

    return df


def _latest_value(series_id: str) -> tuple[pd.Timestamp, float]:
    df = _fetch_fred_series(series_id)
    row = df.iloc[-1]
    return pd.Timestamp(row["date"]), float(row["value"])


def build_fed_rate_monitor_leaf(max_sections: int = 3) -> pd.DataFrame:
    """
    Build a governed Fed rate monitor leaf using FRED only.

    max_sections is retained for interface compatibility with prior CLI/workflow usage.
    """

    upper_date, target_upper = _latest_value(SERIES["target_upper"])
    lower_date, target_lower = _latest_value(SERIES["target_lower"])
    eff_date, effective_rate = _latest_value(SERIES["effective_rate"])

    target_mid = (target_upper + target_lower) / 2.0
    spread_to_mid_bp = (effective_rate - target_mid) * 100.0
    range_width_bp = (target_upper - target_lower) * 100.0
    within_target_range = target_lower <= effective_rate <= target_upper

    as_of_date = max(upper_date, lower_date, eff_date)

    df = pd.DataFrame(
        [
            {
                "date": pd.Timestamp(as_of_date).normalize(),
                "target_lower_pct": round(target_lower, 4),
                "target_upper_pct": round(target_upper, 4),
                "target_mid_pct": round(target_mid, 4),
                "effective_rate_pct": round(effective_rate, 4),
                "spread_to_mid_bp": round(spread_to_mid_bp, 2),
                "range_width_bp": round(range_width_bp, 2),
                "within_target_range": bool(within_target_range),
                "source_target_lower": "FRED:DFEDTARL",
                "source_target_upper": "FRED:DFEDTARU",
                "source_effective_rate": "FRED:DFF",
                "pulled_at_utc": datetime.now(timezone.utc).isoformat(),
            }
        ]
    )

    return df    
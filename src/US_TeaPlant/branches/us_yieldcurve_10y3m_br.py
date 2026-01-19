from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

import pandas as pd


@dataclass(frozen=True)
class YieldCurve10y3mConfig:
    fred_series: str = "T10Y3M"
    start_date: str = "1950-01-01"


def _try_bridge_impl():
    """
    Prefer existing bridge implementation if it already exists.
    This avoids duplicating logic and keeps the leaf->branch contract stable.
    """
    candidates = [
        ("US_TeaPlant.bridges.yc_10y3m_bridge", "get_yc_10y3m_hist"),
        ("US_TeaPlant.bridges.us_yieldcurve_10y3m_bridge", "get_yc_10y3m_hist"),
        ("US_TeaPlant.bridges.yc_10y3m_bridge", "build_yc_10y3m_hist"),
        ("US_TeaPlant.bridges.us_yieldcurve_10y3m_bridge", "build_yc_10y3m_hist"),
    ]
    last_err = None
    for mod_name, fn_name in candidates:
        try:
            mod = __import__(mod_name, fromlist=[fn_name])
            fn = getattr(mod, fn_name)
            return fn
        except Exception as e:
            last_err = e
    return None


def get_yc_10y3m_hist(
    start_date: Optional[str] = None,
    fred_series: str = "T10Y3M",
) -> pd.DataFrame:
    """
    Branch-level source for US 10y-3m spread history.

    Returns a DataFrame with:
      - date (datetime64[ns])
      - value (float)
      - series (str)
    """
    bridge_fn = _try_bridge_impl()
    if bridge_fn is not None:
        df = bridge_fn()  # bridge signature unknown; keep it zero-arg compatible
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Bridge implementation did not return a pandas DataFrame.")
        return df

    api_key = os.environ.get("FRED_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("FRED_API_KEY env var is missing or empty.")

    cfg = YieldCurve10y3mConfig(fred_series=fred_series)
    if start_date:
        cfg = YieldCurve10y3mConfig(fred_series=fred_series, start_date=start_date)

    try:
        from fredapi import Fred
    except Exception as e:
        raise RuntimeError("fredapi is required for FRED pulls in CI.") from e

    fred = Fred(api_key=api_key)
    s = fred.get_series(cfg.fred_series, observation_start=cfg.start_date)

    df = (
        s.rename("value")
        .to_frame()
        .reset_index()
        .rename(columns={"index": "date"})
    )
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["series"] = cfg.fred_series
    df = df.dropna(subset=["value"]).sort_values("date").reset_index(drop=True)
    return df

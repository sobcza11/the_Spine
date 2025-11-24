"""
src/US_TeaPlant/bridges/ir_rates_bridge.py

Builds the canonical IR yields leaf for the Spine:

- Pulls 10Y & policy rates for:
    * USD  (US)
    * EUR  (Euro Area, 19 countries)
    * CAD  (Canada)

- Normalises everything to daily frequency via forward-fill.
- Writes a long, tidy Parquet to R2 at:

    spine_us/us_ir_yields_canonical.parquet

Expected columns (superset for safety):
- ir_date (datetime64[ns])
- as_of_date (datetime64[ns])  # alias of ir_date
- ccy (str)
- tenor (str)   # e.g. "10Y", "POLICY"
- rate (float)  # % level
- yield (float) # alias of rate
"""

from __future__ import annotations

import datetime as dt
import os
from typing import List

import pandas as pd
import requests

from common.r2_client import write_parquet_to_r2


R2_IR_YIELDS_KEY = "spine_us/us_ir_yields_canonical.parquet"

FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"


class FredError(RuntimeError):
    """Raised when FRED returns an error or empty payload."""


def _get_fred_api_key() -> str:
    api_key = os.environ.get("FRED_API_KEY", "").strip()
    if not api_key:
        raise FredError(
            "FRED_API_KEY env var is missing or empty – "
            "set it locally and in GitHub Secrets for the IR workflow."
        )
    return api_key


def _fetch_fred_series(
    series_id: str,
    start_date: dt.date,
    end_date: dt.date,
) -> pd.DataFrame:
    """
    Fetch a single FRED series as a (date, value) DataFrame.

    Dates come back as period dates (daily or month-end, etc.).
    We just parse them & coerce to float here; resampling to daily
    is handled by _resample_to_daily.
    """
    api_key = _get_fred_api_key()

    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start_date.isoformat(),
        "observation_end": end_date.isoformat(),
    }

    resp = requests.get(FRED_BASE_URL, params=params, timeout=15)
    resp.raise_for_status()
    payload = resp.json()

    if "observations" not in payload:
        raise FredError(f"FRED payload missing 'observations' for series {series_id}")

    obs = payload["observations"]
    if not obs:
        raise FredError(f"FRED returned no observations for series {series_id}")

    df = pd.DataFrame(obs)[["date", "value"]].copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["value"])

    return df


def _resample_to_daily(
    df: pd.DataFrame,
    start_date: dt.date,
    end_date: dt.date,
) -> pd.DataFrame:
    """
    Take a (date, value) DF (daily or lower frequency) and produce
    a daily, forward-filled series from start_date to end_date.
    """
    if df.empty:
        return pd.DataFrame(columns=["date", "value"])

    s = (
        df.set_index("date")
        .sort_index()["value"]
    )

    idx = pd.date_range(start=start_date, end=end_date, freq="D")
    s = s.reindex(idx).ffill()

    out = s.reset_index()
    out.columns = ["date", "value"]
    out["date"] = out["date"].dt.date
    return out


def _build_ccy_yields(
    ccy: str,
    tenors: List[str],
    tenors_to_series: dict,
    start_date: dt.date,
    end_date: dt.date,
) -> pd.DataFrame:
    """
    Generic helper: given a mapping {tenor -> FRED series_id}, pull all
    series, resample to daily, & stack them into a long DF for that ccy.
    """
    frames: List[pd.DataFrame] = []

    for tenor in tenors:
        series_id = tenors_to_series[tenor]
        df_raw = _fetch_fred_series(series_id, start_date, end_date)
        df_daily = _resample_to_daily(df_raw, start_date, end_date)
        df_daily["ccy"] = ccy
        df_daily["tenor"] = tenor
        frames.append(df_daily)

    if not frames:
        return pd.DataFrame(columns=["date", "ccy", "tenor", "value"])

    out = pd.concat(frames, ignore_index=True)
    return out


# -------------------------------------------------------------------
# Per-ccy builders
# -------------------------------------------------------------------


def _build_usd_yields(
    start_date: dt.date,
    end_date: dt.date,
) -> pd.DataFrame:
    """
    USD yields via FRED:

    - 10Y:      DGS10        (10-Year Treasury Constant Maturity, daily)
    - POLICY:   FEDFUNDS     (Effective Fed Funds Rate, daily)
    """
    tenors = ["10Y", "POLICY"]
    tenors_to_series = {
        "10Y": "DGS10",
        "POLICY": "FEDFUNDS",
    }

    df = _build_ccy_yields(
        ccy="USD",
        tenors=tenors,
        tenors_to_series=tenors_to_series,
        start_date=start_date,
        end_date=end_date,
    )
    return df


def _build_eur_yields(
    start_date: dt.date,
    end_date: dt.date,
) -> pd.DataFrame:
    """
    EUR-area yields via FRED:

    - 10Y:    IRLTLT01EZM156N  (10Y gov. bond yield, monthly, Euro Area, OECD)
    - POLICY: ECBMRRFR         (ECB Main Refinancing Operations Rate, daily)
    """
    tenors = ["10Y", "POLICY"]
    tenors_to_series = {
        "10Y": "IRLTLT01EZM156N",
        "POLICY": "ECBMRRFR",
    }

    df = _build_ccy_yields(
        ccy="EUR",
        tenors=tenors,
        tenors_to_series=tenors_to_series,
        start_date=start_date,
        end_date=end_date,
    )
    return df


def _build_cad_yields(
    start_date: dt.date,
    end_date: dt.date,
) -> pd.DataFrame:
    """
    CAD yields via FRED:

    - 10Y:    IRLTLT01CAM156N   (10Y gov. bond yield, monthly, Canada, OECD)
    - POLICY: IRSTCB01CAM156N   (Central bank policy rate, monthly, Canada)
    """
    tenors = ["10Y", "POLICY"]
    tenors_to_series = {
        "10Y": "IRLTLT01CAM156N",
        "POLICY": "IRSTCB01CAM156N",
    }

    df = _build_ccy_yields(
        ccy="CAD",
        tenors=tenors,
        tenors_to_series=tenors_to_series,
        start_date=start_date,
        end_date=end_date,
    )
    return df


# -------------------------------------------------------------------
# Public entrypoint
# -------------------------------------------------------------------


def build_ir_yields_canonical(
    start_date: dt.date,
    end_date: dt.date,
) -> pd.DataFrame:
    """
    Build the canonical IR yields leaf across all wired ccys
    and write it to R2.

    Returns the DataFrame that was written.
    """
    print(
        f"[IR-RATES] Building canonical IR yields for "
        f"{start_date.isoformat()}→{end_date.isoformat()} …"
    )

    frames: List[pd.DataFrame] = []

    # USD
    frames.append(_build_usd_yields(start_date, end_date))

    # EUR (Euro Area 19 countries)
    frames.append(_build_eur_yields(start_date, end_date))

    # CAD
    frames.append(_build_cad_yields(start_date, end_date))

    df_all = pd.concat(frames, ignore_index=True)

    # Normalise columns & dtypes
    df_all["ir_date"] = pd.to_datetime(df_all["date"])
    df_all["as_of_date"] = df_all["ir_date"]

    # Rate columns (keep both names for compatibility)
    df_all["rate"] = df_all["value"].astype(float)
    df_all["yield"] = df_all["rate"]

    df_all = df_all[[
        "ir_date",
        "as_of_date",
        "ccy",
        "tenor",
        "rate",
        "yield",
    ]].copy()

    # Sort for stability
    df_all = df_all.sort_values(["ccy", "tenor", "ir_date"]).reset_index(drop=True)

    # Write to R2
    write_parquet_to_r2(df_all, R2_IR_YIELDS_KEY, index=False)

    ccy_count = df_all["ccy"].nunique()
    tenors = sorted(df_all["tenor"].unique())
    print(
        f"[IR-RATES] Built canonical IR yields leaf: "
        f"rows={len(df_all):,}, ccy={ccy_count}, tenors={tenors}"
    )
    print(
        f"[IR-RATES] Wrote canonical IR yields leaf to R2 at "
        f"{R2_IR_YIELDS_KEY} (rows={len(df_all):,})"
    )

    return df_all



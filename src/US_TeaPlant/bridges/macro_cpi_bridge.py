# src/US_TeaPlant/bridges/macro_cpi_bridge.py

"""
US_TeaPlant.bridges.macro_cpi_bridge

Builds the canonical CPI (inflation) leaf for the_Spine using TradingEconomics.

Schema (canonical CPI leaf):

    as_of_date      datetime64[ns]
    ccy             str         # e.g. 'USD'
    cpi_yoy         float       # YoY CPI inflation (%)
    leaf_group      str         # 'MACRO'
    leaf_name       str         # 'macro_cpi_canonical'
    source_system   str         # 'TRADINGECONOMICS'
    updated_at      datetime64[ns]

R2 key:

    spine_us/us_macro_cpi_canonical.parquet

Notes
-----
- This is intentionally minimal: YoY CPI only.
- Once TradingEconomics access is live, this can be extended
  to include MoM, core CPI, etc.
"""

from __future__ import annotations

import datetime as dt
import os
from dataclasses import dataclass
from typing import List

import pandas as pd
import requests

from common.r2_client import write_parquet_to_r2

# ---------------------------------------------------------------------------
# R2 + API config
# ---------------------------------------------------------------------------

R2_CPI_KEY = "spine_us/us_macro_cpi_canonical.parquet"

TE_BASE_URL = "https://api.tradingeconomics.com/historical/country"
TE_TIMEOUT_SEC = 30


class TradingEconomicsError(RuntimeError):
    """Custom error for TradingEconomics-related issues."""


@dataclass(frozen=True)
class CpiSeriesConfig:
    """
    Configuration for a single CPI series.
    """
    ccy: str            # 'USD', 'EUR', ...
    country: str        # 'united states', 'euro area', ...
    indicator: str      # 'inflation rate', etc.


def _get_te_api_key() -> str:
    """
    Read TradingEconomics API key from env.

    Expectation: TRADINGECONOMICS_API_KEY contains "key:secret"
    as required by the TE Python client / HTTP API.
    """
    api_key = os.environ.get("TRADINGECONOMICS_API_KEY", "").strip()
    if not api_key:
        raise TradingEconomicsError(
            "TRADINGECONOMICS_API_KEY env var is missing or empty – "
            "set it locally and in CI for macro CPI workflow."
        )
    return api_key


def _fetch_te_cpi_series(
    cfg: CpiSeriesConfig,
    start_date: dt.date,
    end_date: dt.date,
) -> pd.DataFrame:
    """
    Fetch CPI series from TradingEconomics historical endpoint.

    Endpoint pattern (per TE docs):
        /historical/country/{country}/indicator/{indicator}/{start}/{end}?c=APIKEY

    Response JSON has fields like:
        Country, Category, DateTime, Value, Frequency, HistoricalDataSymbol, LastUpdate
    """
    api_key = _get_te_api_key()

    start_iso = start_date.isoformat()
    end_iso = end_date.isoformat()

    url = (
        f"{TE_BASE_URL}/{cfg.country}/indicator/{cfg.indicator}/"
        f"{start_iso}/{end_iso}"
    )
    params = {
        "c": api_key,
        "f": "json",
    }

    print(
        f"[MACRO-CPI] Fetching CPI for {cfg.ccy} "
        f"(country={cfg.country}, indicator={cfg.indicator}) "
        f"{start_iso}→{end_iso} …"
    )

    resp = requests.get(url, params=params, timeout=TE_TIMEOUT_SEC)
    try:
        resp.raise_for_status()
    except requests.HTTPError as exc:
        raise TradingEconomicsError(
            f"TradingEconomics HTTP error for {cfg.ccy} "
            f"(country={cfg.country}, indicator={cfg.indicator}): {exc}"
        ) from exc

    data = resp.json()
    if not data:
        print(
            f"[MACRO-CPI] WARNING: TE returned 0 rows for {cfg.ccy} "
            f"(country={cfg.country}, indicator={cfg.indicator})."
        )
        return pd.DataFrame(columns=["DateTime", "Value"])

    df = pd.DataFrame(data)

    # TE may use 'Value' or 'Close' depending on endpoint flavor
    if "Value" in df.columns:
        value_col = "Value"
    elif "Close" in df.columns:
        value_col = "Close"
    else:
        raise TradingEconomicsError(
            f"Unable to find CPI value column for {cfg.ccy}; "
            f"expected 'Value' or 'Close', got {df.columns.tolist()}"
        )

    # Keep only DateTime + value
    df = df[["DateTime", value_col]].rename(
        columns={"DateTime": "as_of_date", value_col: "cpi_yoy"}
    )

    # Normalize types
    df["as_of_date"] = pd.to_datetime(df["as_of_date"], errors="coerce")
    df["cpi_yoy"] = pd.to_numeric(df["cpi_yoy"], errors="coerce")

    df = df.dropna(subset=["as_of_date", "cpi_yoy"]).sort_values("as_of_date")

    return df


def _get_cpi_series_configs() -> List[CpiSeriesConfig]:
    """
    CPI configs for the core G10 set currently wired in the Spine.

    NOTE:
    - 'country' + 'indicator' strings must match TradingEconomics naming
      (these may need small adjustments once you confirm with their docs).
    """
    return [
        CpiSeriesConfig(ccy="USD", country="united states", indicator="inflation rate"),
        CpiSeriesConfig(ccy="EUR", country="euro area", indicator="inflation rate"),
        CpiSeriesConfig(ccy="JPY", country="japan", indicator="inflation rate"),
        CpiSeriesConfig(ccy="GBP", country="united kingdom", indicator="inflation rate"),
        CpiSeriesConfig(ccy="CAD", country="canada", indicator="inflation rate"),
        CpiSeriesConfig(ccy="AUD", country="australia", indicator="inflation rate"),
        CpiSeriesConfig(ccy="NZD", country="new zealand", indicator="inflation rate"),
        CpiSeriesConfig(ccy="CHF", country="switzerland", indicator="inflation rate"),
        CpiSeriesConfig(ccy="SEK", country="sweden", indicator="inflation rate"),
        CpiSeriesConfig(ccy="NOK", country="norway", indicator="inflation rate"),
    ]


def build_macro_cpi_canonical(
    start_date: dt.date,
    end_date: dt.date,
) -> pd.DataFrame:
    """
    Build the canonical CPI (inflation) leaf across configured CCYs,
    write it to R2, and return the DataFrame.

    This is the macro CPI leaf for the_Spine.
    """
    print(
        f"[MACRO-CPI] Building canonical CPI leaf for "
        f"{start_date}→{end_date} …"
    )

    updated_at = pd.Timestamp.utcnow()
    frames: List[pd.DataFrame] = []

    for cfg in _get_cpi_series_configs():
        df_cpi = _fetch_te_cpi_series(cfg, start_date, end_date)
        if df_cpi.empty:
            print(f"[MACRO-CPI] WARNING: No CPI rows for {cfg.ccy}, skipping.")
            continue

        df_cpi["ccy"] = cfg.ccy
        df_cpi["leaf_group"] = "MACRO"
        df_cpi["leaf_name"] = "macro_cpi_canonical"
        df_cpi["source_system"] = "TRADINGECONOMICS"
        df_cpi["updated_at"] = updated_at

        frames.append(df_cpi)

    if not frames:
        print("[MACRO-CPI] WARNING: No CPI data built for any CCY.")
        # Return an empty but schema-consistent frame
        df_empty = pd.DataFrame(
            columns=[
                "as_of_date",
                "ccy",
                "cpi_yoy",
                "leaf_group",
                "leaf_name",
                "source_system",
                "updated_at",
            ]
        )
        write_parquet_to_r2(df_empty, R2_CPI_KEY, index=False)
        print(
            f"[MACRO-CPI] Wrote EMPTY CPI canonical leaf to R2 at {R2_CPI_KEY} "
            "(rows=0)"
        )
        return df_empty

    df_all = pd.concat(frames, ignore_index=True)
    df_all = df_all.sort_values(["as_of_date", "ccy"]).reset_index(drop=True)

    print(
        f"[MACRO-CPI] Built canonical CPI leaf: "
        f"rows={len(df_all):,}, ccy={df_all['ccy'].nunique()}"
    )

    write_parquet_to_r2(df_all, R2_CPI_KEY, index=False)
    print(
        f"[MACRO-CPI] Wrote canonical CPI leaf to R2 at {R2_CPI_KEY} "
        f"(rows={len(df_all):,})"
    )

    return df_all


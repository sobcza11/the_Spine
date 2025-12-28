"""
Fetch specific ETF benchmarks from EODHD into data_yahoo_combined/
for use in VinV_1_0.

This replaces Yahoo for ETF benchmarks only, without changing the
VinV_1_0 core logic. It writes *_combined.csv files in the same
schema the existing loader expects.

Required env var:
    EODHD_API_TOKEN

EODHD docs:
    - EOD endpoint: https://eodhd.com/api/eod/VOO.US?api_token=...&fmt=json
    - Dividends:    https://eodhd.com/api/div/VOO.US?from=1970-01-01&api_token=...&fmt=json
"""

from __future__ import annotations

import os
import time
from io import StringIO
from pathlib import Path
from typing import List

import pandas as pd
import requests


# --------------------------
# CONFIG
# --------------------------

# Core benchmark set for VinV_1_0
BENCHMARK_TICKERS: List[str] = ["VOO", "VTV", "IVE", "VIG", "SCHD"]

EODHD_API_TOKEN = os.environ.get("6428a3c56c2414.06336538")
if not EODHD_API_TOKEN:
    raise RuntimeError(
        "EODHD_API_TOKEN env var not set. "
        "Get your API key from eodhd.com & set it before running this script."
    )

BASE_EOD_URL = "https://eodhd.com/api/eod/{symbol}.US"
BASE_DIV_URL = "https://eodhd.com/api/div/{symbol}.US"

# Simple retry settings
MAX_RETRIES = 5
BASE_DELAY = 3.0  # seconds


# --------------------------
# HTTP helpers
# --------------------------

def _request_json(url: str, label: str) -> list[dict]:
    """
    Basic GET with retry/backoff that returns JSON list.
    On repeated failure, returns an empty list.
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            resp = requests.get(url, timeout=20)
            status = resp.status_code

            if status == 200:
                data = resp.json()
                if isinstance(data, list):
                    return data
                # Some EODHD endpoints can wrap data differently;
                # we normalize to a list of dicts here.
                if isinstance(data, dict):
                    if "data" in data and isinstance(data["data"], list):
                        return data["data"]
                return []

            if status == 429:
                if attempt > MAX_RETRIES:
                    print(f"[ERROR] {label}: 429 Too Many Requests after {MAX_RETRIES} attempts. Skipping.")
                    return []
                delay = BASE_DELAY * (2 ** (attempt - 1))
                print(f"[RATE-LIMIT] {label}: 429 on attempt {attempt}/{MAX_RETRIES}. Sleeping {delay:.1f}s.")
                time.sleep(delay)
                continue

            # Other 4xx/5xx
            print(f"[ERROR] {label}: HTTP {status} for URL:\n       {url}")
            if attempt > MAX_RETRIES:
                print(f"[ERROR] {label}: giving up after {MAX_RETRIES} attempts.")
                return []
            delay = BASE_DELAY * (2 ** (attempt - 1))
            print(f"[RETRY] {label}: attempt {attempt}/{MAX_RETRIES}, sleeping {delay:.1f}s.")
            time.sleep(delay)

        except Exception as exc:
            if attempt > MAX_RETRIES:
                print(f"[ERROR] {label}: exception after {MAX_RETRIES} attempts: {exc}")
                return []
            delay = BASE_DELAY * (2 ** (attempt - 1))
            print(f"[EXCEPTION] {label}: {exc}. attempt {attempt}/{MAX_RETRIES}, sleeping {delay:.1f}s.")
            time.sleep(delay)


# --------------------------
# EODHD fetch logic
# --------------------------

def fetch_eod(ticker: str) -> pd.DataFrame:
    """
    Fetch full EOD history for a ticker from EODHD.

    We keep the adjusted_close (if present) & later map it to 'price'
    to stay compatible with VinV_1_0's combined schema.
    """
    url = BASE_EOD_URL.format(symbol=ticker) + f"?api_token={EODHD_API_TOKEN}&fmt=json"
    label = f"{ticker} EOD"
    print(f"[EOD] {label} -> {url}")
    rows = _request_json(url, label)
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    # Standardize columns
    df.columns = [c.lower() for c in df.columns]

    # Ensure date sorted ascending
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")

    return df


def fetch_dividends(ticker: str) -> pd.DataFrame:
    """
    Fetch dividend history from EODHD.

    For paid plans, you can go back decades; for free, EODHD may
    restrict to ~1 year of history. We still normalize to a simple
    (date, dividend) table.
    """
    # Use a very early 'from' date; EODHD will respect plan limits.
    url = BASE_DIV_URL.format(symbol=ticker) + f"?from=1970-01-01&api_token={EODHD_API_TOKEN}&fmt=json"
    label = f"{ticker} DIV"
    print(f"[DIV] {label} -> {url}")
    rows = _request_json(url, label)
    if not rows:
        print(f"[INFO] {ticker}: no dividend rows returned (or request failed).")
        return pd.DataFrame(columns=["date", "dividend"])

    df = pd.DataFrame(rows)
    df.columns = [c.lower() for c in df.columns]
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")

    # EODHD uses 'value' or 'dividend' depending on endpoint; normalize:
    if "dividend" in df.columns:
        val_col = "dividend"
    elif "value" in df.columns:
        val_col = "value"
    else:
        return pd.DataFrame(columns=["date", "dividend"])

    return df[["date", val_col]].rename(columns={val_col: "dividend"})


def main() -> None:
    # We are in VinV_1_0/scripts/, so root is parent
    root = Path(__file__).resolve().parents[1]
    combined_dir = root / "data_yahoo_combined"
    combined_dir.mkdir(parents=True, exist_ok=True)

    print(f"[INFO] VinV_1_0 root: {root}")
    print(f"[INFO] Writing combined ETF files to: {combined_dir}")
    print(f"[INFO] Tickers: {BENCHMARK_TICKERS}")

    for ticker in BENCHMARK_TICKERS:
        print(f"\n[FETCH] {ticker}")

        df_eod = fetch_eod(ticker)
        if df_eod.empty:
            print(f"[WARN] {ticker}: no EOD data. Skipping.")
            continue

        df_div = fetch_dividends(ticker)

        # Merge price & dividend
        df = df_eod.copy()

        # Ensure we have a 'dividend' column to merge
        if not df_div.empty:
            df = df.merge(df_div, on="date", how="left")
        else:
            df["dividend"] = 0.0

        # If EODHD gives 'adjusted_close', we prefer that for total return
        # Otherwise fall back to 'close'
        cols = [c for c in df.columns]
        price_col = None
        if "adjusted_close" in cols:
            price_col = "adjusted_close"
        elif "adj_close" in cols:
            price_col = "adj_close"
        elif "close" in cols:
            price_col = "close"

        if price_col is None:
            print(f"[ERROR] {ticker}: no suitable price column in {cols}. Skipping.")
            continue

        df = df[["date", price_col, "dividend"]].copy()
        df["ticker"] = ticker
        df = df.rename(columns={price_col: "price"})

        out_path = combined_dir / f"{ticker}_combined.csv"
        df.to_csv(out_path, index=False)
        print(f"[SAVE] {ticker}: {out_path}")


if __name__ == "__main__":
    main()


"""
Fetch ETF dividends from Yahoo Finance & merge into existing
*_combined.csv files produced by EODHD price fetch.

Assumptions:
- There is already a file VinV_1_0/data_yahoo_combined/{TICKER}_combined.csv
  with columns: date, price, dividend, ticker
- We only UPDATE the 'dividend' column based on Yahoo's Dividends CSV.

Yahoo endpoints used:
  https://query1.finance.yahoo.com/v7/finance/download/{TICKER}
    ?period1=0&period2=9999999999&interval=1d&events=div&includeAdjustedClose=true
"""

from __future__ import annotations

from pathlib import Path
from typing import List
from io import StringIO

import pandas as pd
import requests
import time


# --------------------------
# CONFIG
# --------------------------

BENCHMARK_TICKERS: List[str] = ["VOO", "VTV", "IVE", "VIG", "SCHD"]

BASE_YF_URL = (
    "https://query1.finance.yahoo.com/v7/finance/download/"
    "{ticker}?period1=0&period2=9999999999&interval=1d&events=div&includeAdjustedClose=true"
)

MAX_RETRIES = 5
BASE_DELAY = 3.0  # seconds


# --------------------------
# HTTP helper
# --------------------------

def _fetch_yahoo_div_csv(ticker: str) -> pd.DataFrame:
    """
    Fetch dividend CSV from Yahoo with basic retry/backoff.
    Returns DataFrame with columns: date, dividends (if available)
    or empty DataFrame if no data.
    """
    url = BASE_YF_URL.format(ticker=ticker)
    label = f"{ticker} YF DIV"

    attempt = 0
    while True:
        attempt += 1
        try:
            resp = requests.get(url, timeout=20)
            status = resp.status_code

            if status == 200:
                # Yahoo returns CSV text
                csv_buf = StringIO(resp.text)
                df = pd.read_csv(csv_buf)
                if df.empty:
                    print(f"[INFO] {label}: empty CSV from Yahoo.")
                    return pd.DataFrame(columns=["date", "dividend"])

                df.columns = [c.lower() for c in df.columns]
                # Expect "date" & "dividends"
                if "date" not in df.columns:
                    print(f"[WARN] {label}: no 'date' column in Yahoo CSV. Columns: {df.columns}")
                    return pd.DataFrame(columns=["date", "dividend"])

                df["date"] = pd.to_datetime(df["date"])
                if "dividends" in df.columns:
                    df = df[["date", "dividends"]].rename(columns={"dividends": "dividend"})
                else:
                    # Sometimes Yahoo might send no dividends col
                    print(f"[WARN] {label}: no 'dividends' column. Columns: {df.columns}")
                    return pd.DataFrame(columns=["date", "dividend"])

                df = df.sort_values("date")
                return df

            if status == 404:
                print(f"[INFO] {label}: 404 (no dividend history).")
                return pd.DataFrame(columns=["date", "dividend"])

            if status == 429:
                if attempt > MAX_RETRIES:
                    print(f"[ERROR] {label}: 429 after {MAX_RETRIES} attempts. Giving up.")
                    return pd.DataFrame(columns=["date", "dividend"])
                delay = BASE_DELAY * (2 ** (attempt - 1))
                print(f"[RATE-LIMIT] {label}: 429 on attempt {attempt}/{MAX_RETRIES}. Sleeping {delay:.1f}s.")
                time.sleep(delay)
                continue

            print(f"[ERROR] {label}: HTTP {status}. URL:\n       {url}")
            if attempt > MAX_RETRIES:
                print(f"[ERROR] {label}: giving up after {MAX_RETRIES} attempts.")
                return pd.DataFrame(columns=["date", "dividend"])
            delay = BASE_DELAY * (2 ** (attempt - 1))
            print(f"[RETRY] {label}: attempt {attempt}/{MAX_RETRIES}, sleeping {delay:.1f}s.")
            time.sleep(delay)

        except Exception as exc:
            if attempt > MAX_RETRIES:
                print(f"[ERROR] {label}: exception after {MAX_RETRIES} attempts: {exc}")
                return pd.DataFrame(columns=["date", "dividend"])
            delay = BASE_DELAY * (2 ** (attempt - 1))
            print(f"[EXCEPTION] {label}: {exc}. attempt {attempt}/{MAX_RETRIES}, sleeping {delay:.1f}s.")
            time.sleep(delay)


# --------------------------
# Main
# --------------------------

def main() -> None:
    root = Path(__file__).resolve().parents[1]  # VinV_1_0/
    combined_dir = root / "data_yahoo_combined"

    print(f"[INFO] VinV_1_0 root: {root}")
    print(f"[INFO] Updating dividends in: {combined_dir}")
    print(f"[INFO] Benchmark tickers: {BENCHMARK_TICKERS}")

    for ticker in BENCHMARK_TICKERS:
        print(f"\n[DIV-MERGE] {ticker}")

        combined_path = combined_dir / f"{ticker}_combined.csv"
        if not combined_path.exists():
            print(f"[WARN] {ticker}: combined file not found at {combined_path}. Skipping.")
            continue

        df_comb = pd.read_csv(combined_path)
        df_comb.columns = [c.lower() for c in df_comb.columns]
        if "date" not in df_comb.columns:
            print(f"[ERROR] {ticker}: combined file missing 'date' column. Columns: {df_comb.columns}")
            continue

        df_comb["date"] = pd.to_datetime(df_comb["date"])

        df_div = _fetch_yahoo_div_csv(ticker)
        if df_div.empty:
            print(f"[INFO] {ticker}: no Yahoo dividends to merge. Leaving dividend column as-is.")
            df_comb.to_csv(combined_path, index=False)
            continue

        # Merge: prefer Yahoo dividends where present
        df_merged = df_comb.merge(df_div, on="date", how="left", suffixes=("", "_yf"))

        # If both 'dividend' (from EOD placeholder) & 'dividend_yf' exist:
        if "dividend_yf" in df_merged.columns:
            # Use Yahoo where not null, else keep existing dividend
            df_merged["dividend"] = df_merged["dividend_yf"].fillna(df_merged.get("dividend", 0.0))
            df_merged = df_merged.drop(columns=["dividend_yf"])

        df_merged = df_merged.sort_values("date")

        df_merged.to_csv(combined_path, index=False)
        print(f"[SAVE] {ticker}: updated dividends in {combined_path}")


if __name__ == "__main__":
    main()

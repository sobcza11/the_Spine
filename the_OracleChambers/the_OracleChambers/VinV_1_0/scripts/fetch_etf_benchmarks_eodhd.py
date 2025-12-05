"""
Fetch specific ETF benchmarks from Yahoo Finance (prices + dividends)
and write them into data_yahoo_combined/ as *_combined.csv files
compatible with VinV_1_0.

Tickers:
    VOO, VTV, IVE, VIG, SCHD

Yahoo CSV endpoints:
    Prices (history):
      https://query1.finance.yahoo.com/v7/finance/download/{TICKER}
        ?period1=0&period2=9999999999&interval=1d&events=history&includeAdjustedClose=true

    Dividends:
      https://query1.finance.yahoo.com/v7/finance/download/{TICKER}
        ?period1=0&period2=9999999999&interval=1d&events=div&includeAdjustedClose=true

Output schema per file:
    date, price, dividend, ticker

This matches what VinV_1_0's loader expects (with 'price' being adj_close).
"""

from __future__ import annotations

import time
from io import StringIO
from pathlib import Path
from typing import List

import pandas as pd
import requests


# --------------------------
# CONFIG
# --------------------------

BENCHMARK_TICKERS: List[str] = ["VOO", "VTV", "IVE", "VIG", "SCHD"]

BASE_YF_PRICE_URL = (
    "https://query1.finance.yahoo.com/v7/finance/download/"
    "{ticker}?period1=0&period2=9999999999&interval=1d&events=history&includeAdjustedClose=true"
)

BASE_YF_DIV_URL = (
    "https://query1.finance.yahoo.com/v7/finance/download/"
    "{ticker}?period1=0&period2=9999999999&interval=1d&events=div&includeAdjustedClose=true"
)

MAX_RETRIES = 5
BASE_DELAY = 3.0      # seconds for exponential backoff
SLEEP_BETWEEN_TICKERS = 5.0  # extra politeness pause between tickers

# Simple headers to look less like a bot
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; VinV_1_0/1.0; +https://github.com/)",
}


# --------------------------
# HTTP helpers
# --------------------------

def _fetch_yahoo_csv(url: str, label: str) -> pd.DataFrame:
    """
    Generic CSV fetcher with retry/backoff.
    Returns DataFrame or empty DataFrame on failure.
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            resp = requests.get(url, headers=DEFAULT_HEADERS, timeout=20)
            status = resp.status_code

            if status == 200:
                csv_buf = StringIO(resp.text)
                df = pd.read_csv(csv_buf)
                return df

            # Too many requests: backoff then retry
            if status == 429:
                if attempt > MAX_RETRIES:
                    print(f"[ERROR] {label}: 429 after {MAX_RETRIES} attempts. Giving up.")
                    return pd.DataFrame()
                delay = BASE_DELAY * (2 ** (attempt - 1))
                print(f"[RATE-LIMIT] {label}: 429 on attempt {attempt}/{MAX_RETRIES}. Sleeping {delay:.1f}s.")
                time.sleep(delay)
                continue

            # 404: no data
            if status == 404:
                print(f"[INFO] {label}: 404 (no data).")
                return pd.DataFrame()

            print(f"[ERROR] {label}: HTTP {status}. URL:\n       {url}")
            if attempt > MAX_RETRIES:
                print(f"[ERROR] {label}: giving up after {MAX_RETRIES} attempts.")
                return pd.DataFrame()
            delay = BASE_DELAY * (2 ** (attempt - 1))
            print(f"[RETRY] {label}: attempt {attempt}/{MAX_RETRIES}, sleeping {delay:.1f}s.")
            time.sleep(delay)

        except Exception as exc:
            if attempt > MAX_RETRIES:
                print(f"[ERROR] {label}: exception after {MAX_RETRIES} attempts: {exc}")
                return pd.DataFrame()
            delay = BASE_DELAY * (2 ** (attempt - 1))
            print(f"[EXCEPTION] {label}: {exc}. attempt {attempt}/{MAX_RETRIES}, sleeping {delay:.1f}s.")
            time.sleep(delay)


def fetch_price_history(ticker: str) -> pd.DataFrame:
    """
    Fetch full daily price history for ticker from Yahoo.
    Uses Adj Close as 'price' when available.
    """
    url = BASE_YF_PRICE_URL.format(ticker=ticker)
    label = f"{ticker} PRICE"
    print(f"[PRICE] {label} -> {url}")

    df = _fetch_yahoo_csv(url, label)
    if df.empty:
        return pd.DataFrame()

    df.columns = [c.lower() for c in df.columns]

    if "date" not in df.columns:
        print(f"[ERROR] {label}: no 'Date' column in CSV. Columns: {df.columns}")
        return pd.DataFrame()

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    # Prefer adj close, fallback to close
    price_col = None
    if "adj close" in df.columns:
        price_col = "adj close"
    elif "adj_close" in df.columns:
        price_col = "adj_close"
    elif "close" in df.columns:
        price_col = "close"

    if price_col is None:
        print(f"[ERROR] {label}: no suitable price column in {df.columns}")
        return pd.DataFrame()

    return df[["date", price_col]].rename(columns={price_col: "price"})


def fetch_dividends(ticker: str) -> pd.DataFrame:
    """
    Fetch dividend history from Yahoo.
    Returns columns: date, dividend
    """
    url = BASE_YF_DIV_URL.format(ticker=ticker)
    label = f"{ticker} DIV"
    print(f"[DIV] {label} -> {url}")

    df = _fetch_yahoo_csv(url, label)
    if df.empty:
        print(f"[INFO] {label}: no dividend data from Yahoo.")
        return pd.DataFrame(columns=["date", "dividend"])

    df.columns = [c.lower() for c in df.columns]

    if "date" not in df.columns:
        print(f"[WARN] {label}: no 'Date' column. Columns: {df.columns}")
        return pd.DataFrame(columns=["date", "dividend"])

    df["date"] = pd.to_datetime(df["date"])
    if "dividends" not in df.columns:
        print(f"[WARN] {label}: no 'Dividends' column. Columns: {df.columns}")
        return pd.DataFrame(columns=["date", "dividend"])

    df = df[["date", "dividends"]].rename(columns={"dividends": "dividend"})
    df = df.sort_values("date")

    return df


# --------------------------
# Main
# --------------------------

def main() -> None:
    root = Path(__file__).resolve().parents[1]   # VinV_1_0/
    combined_dir = root / "data_yahoo_combined"
    combined_dir.mkdir(parents=True, exist_ok=True)

    print(f"[INFO] VinV_1_0 root: {root}")
    print(f"[INFO] Writing ETF combined files to: {combined_dir}")
    print(f"[INFO] Benchmark tickers: {BENCHMARK_TICKERS}")

    for idx, ticker in enumerate(BENCHMARK_TICKERS, start=1):
        print(f"\n[FETCH] {idx}/{len(BENCHMARK_TICKERS)} {ticker}")

        df_price = fetch_price_history(ticker)
        if df_price.empty:
            print(f"[WARN] {ticker}: no price data. Skipping.")
            continue

        df_div = fetch_dividends(ticker)

        df = df_price.copy()
        if not df_div.empty:
            df = df.merge(df_div, on="date", how="left")
        else:
            df["dividend"] = 0.0

        df["ticker"] = ticker
        df = df.sort_values("date")

        out_path = combined_dir / f"{ticker}_combined.csv"
        df.to_csv(out_path, index=False)
        print(f"[SAVE] {ticker}: {out_path}")

        # Be polite to Yahoo
        if idx < len(BENCHMARK_TICKERS):
            print(f"[SLEEP] Sleeping {SLEEP_BETWEEN_TICKERS:.1f}s before next ticker...")
            time.sleep(SLEEP_BETWEEN_TICKERS)


if __name__ == "__main__":
    main()

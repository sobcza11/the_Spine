"""
vinv_stooq_test.py

Quick test script to pull historical OHLCV data from Stooq.com
and save it locally for inspection.

Usage:
    1) pip install requests pandas
    2) python vinv_stooq_test.py
"""

import os
import sys
import time
from typing import List

import requests
import pandas as pd


def build_stooq_symbol(ticker: str, exchange_suffix: str = "US") -> str:
    """
    Build the Stooq symbol for a given ticker.

    Examples:
        "AAPL" -> "aapl.us"
        "MSFT" -> "msft.us"
    """
    ticker = ticker.strip().lower()
    suffix = exchange_suffix.strip().lower()
    return f"{ticker}.{suffix}"


def normalize_stooq_columns(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    Normalize Stooq columns to: Date, Open, High, Low, Close, Volume.

    Stooq sometimes returns Polish headers:
        Data, Otwarcie, Najwyzszy, Najnizszy, Zamkniecie, Wolumen

    and sometimes English:
        Date, Open, High, Low, Close, Volume
    """

    cols = list(df.columns)

    # Case 1: Already English
    english_set = {"Date", "Open", "High", "Low", "Close", "Volume"}
    if english_set.issubset(set(cols)):
        return df

    # Case 2: Polish headers
    polish_map = {
        "Data": "Date",
        "Otwarcie": "Open",
        "Najwyzszy": "High",
        "Najnizszy": "Low",
        "Zamkniecie": "Close",
        "Wolumen": "Volume",
    }

    if set(polish_map.keys()).issubset(set(cols)):
        df = df.rename(columns=polish_map)
        return df

    # If we get here, it’s something unexpected
    raise RuntimeError(
        f"Unexpected columns for {ticker}: {cols}. "
        f"Could not match either English or Polish schema."
    )


def fetch_stooq_ohlcv(
    ticker: str,
    interval: str = "d",
    exchange_suffix: str = "US",
    timeout: int = 10,
) -> pd.DataFrame:
    """
    Fetch OHLCV history for a single equity from Stooq.

    Stooq URL format:
        https://stooq.pl/q/d/l/?s={symbol}&i={interval}
    """
    symbol = build_stooq_symbol(ticker, exchange_suffix=exchange_suffix)
    url = f"https://stooq.pl/q/d/l/?s={symbol}&i={interval}"

    print(f"[INFO] Fetching {ticker} from Stooq: {url}")
    resp = requests.get(url, timeout=timeout)

    if resp.status_code != 200:
        raise RuntimeError(
            f"Failed to fetch data for {ticker}. "
            f"HTTP {resp.status_code} - {resp.text[:200]}"
        )

    # Stooq returns CSV content
    try:
        from io import StringIO

        df = pd.read_csv(StringIO(resp.text))
    except Exception as e:
        raise RuntimeError(f"Failed to parse CSV for {ticker}: {e}")

    # Normalize column names (Polish -> English if needed)
    df = normalize_stooq_columns(df, ticker)

    # Basic sanity check
    expected_cols = {"Date", "Open", "High", "Low", "Close", "Volume"}
    if not expected_cols.issubset(set(df.columns)):
        raise RuntimeError(
            f"Unexpected columns after normalization for {ticker}: {df.columns.tolist()}"
        )

    # Convert Date to datetime & sort
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values("Date").reset_index(drop=True)

    print(
        f"[INFO] {ticker}: {len(df):,} rows from "
        f"{df['Date'].min().date()} to {df['Date'].max().date()}"
    )

    return df


def save_dataframe_csv(df: pd.DataFrame, ticker: str, base_dir: str = "data_stooq") -> str:
    """
    Save DataFrame to CSV in a simple, test-friendly folder.
    """
    os.makedirs(base_dir, exist_ok=True)
    filename = f"{ticker.upper()}_stooq.csv"
    path = os.path.join(base_dir, filename)
    df.to_csv(path, index=False)
    print(f"[INFO] Saved {ticker} data to: {path}")
    return path


def fetch_universe(
    tickers: List[str],
    interval: str = "d",
    exchange_suffix: str = "US",
    sleep_seconds: float = 1.0,
) -> None:
    """
    Fetch and save OHLCV data for a list of tickers.
    """
    for i, ticker in enumerate(tickers, start=1):
        print(f"\n[UNIVERSE] ({i}/{len(tickers)}) {ticker}")
        try:
            df = fetch_stooq_ohlcv(
                ticker=ticker,
                interval=interval,
                exchange_suffix=exchange_suffix,
            )
            save_dataframe_csv(df, ticker)
        except Exception as e:
            print(f"[ERROR] Ticker {ticker}: {e}")
        time.sleep(sleep_seconds)


def quick_single_ticker_test(ticker: str = "AAPL") -> None:
    """
    Simple test: fetch one ticker, show head & tail.
    """
    df = fetch_stooq_ohlcv(ticker)
    print("\n[HEAD]")
    print(df.head())
    print("\n[TAIL]")
    print(df.tail())
    save_dataframe_csv(df, ticker)


if __name__ == "__main__":
    # --- Option 1: Quick single-ticker smoke test -------------------------
    # Uncomment this to just test one ticker quickly:
    #
    # quick_single_ticker_test("AAPL")
    # sys.exit(0)

    # --- Option 2: Small universe test -----------------------------------
    test_tickers = [
        "AAPL",
        "MSFT",
        "XOM",
        "JPM",
        "BRK-B",  # you'll handle any special mapping in build_stooq_symbol if needed
    ]

    fetch_universe(
        tickers=test_tickers,
        interval="d",        # "d" = daily, "w" = weekly, "m" = monthly
        exchange_suffix="US",
        sleep_seconds=1.0,   # be polite
    )

    print("\n[DONE] Stooq primary-source test completed.")


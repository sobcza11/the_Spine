"""
yahoo_vinv_fetcher.py

Yahoo-only data engine for VinV:

- Uses Selenium (headless) to load Yahoo Finance pages.
- Pulls full daily price history for each ticker.
- Pulls dividend history for each ticker.
- Merges them into a single DataFrame with a Dividend column.
- Saves per-ticker CSV to data_yahoo_combined/.

Requirements:
    pip install selenium webdriver-manager pandas
"""

import os
import time
import random
from io import StringIO
from typing import Optional, List

import argparse
import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# -----------------------------
# CONFIG
# -----------------------------

DATA_DIR_PRICES = "data_yahoo_prices"
DATA_DIR_DIVS = "data_yahoo_dividends"
DATA_DIR_COMBINED = "data_yahoo_combined"

BASE_DELAY_SECONDS = 5.0       # base sleep after each page load
EXTRA_DELAY_SECONDS = 3.0      # extra sleep between tickers

PAUSE_EVERY_N_TICKERS = 50     # long pause every N tickers
PAUSE_MIN_SECONDS = 120        # 2 minutes
PAUSE_MAX_SECONDS = 300        # 5 minutes


# -----------------------------
# ARGUMENTS
# -----------------------------

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument(
        "--universe",
        type=str,
        default="df_zacks.csv",
        help="Path to CSV with at least a 'Ticker' or 'ticker' column.",
    )
    p.add_argument(
        "--no-long-pauses",
        action="store_true",
        help="Disable long pauses every N tickers (still keeps per-page delays).",
    )
    return p.parse_args()


# -----------------------------
# SELENIUM SETUP
# -----------------------------

def create_webdriver(headless: bool = True) -> webdriver.Chrome:
    """
    Create a Chrome WebDriver using webdriver-manager.

    Headless by default to run quietly in the background.
    """
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-dev-shm-usage")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver


def build_driver() -> webdriver.Chrome:
    """
    Wrapper for creating the Selenium driver.
    """
    return create_webdriver(headless=True)


# -----------------------------
# YAHOO URL HELPERS
# -----------------------------

def yahoo_history_url(ticker: str) -> str:
    """
    Full daily history URL (max range) for a ticker.
    """
    base = "https://finance.yahoo.com/quote/{ticker}/history"
    params = (
        "?period1=0"
        "&period2=9999999999"
        "&interval=1d"
        "&filter=history"
        "&frequency=1d"
        "&includeAdjustedClose=true"
    )
    return base.format(ticker=ticker) + params


def yahoo_dividends_url(ticker: str) -> str:
    """
    Dividends-only history URL for a ticker.
    """
    base = "https://finance.yahoo.com/quote/{ticker}/history"
    params = (
        "?period1=0"
        "&period2=9999999999"
        "&interval=1d"
        "&filter=dividends"
        "&frequency=1d"
        "&includeAdjustedClose=true"
    )
    return base.format(ticker=ticker) + params


# -----------------------------
# PARSERS
# -----------------------------

def parse_yahoo_price_table(html: str, ticker: str) -> pd.DataFrame:
    """
    Parse daily price history table from Yahoo HTML into a clean DataFrame.

    Robust version:
    - Uses StringIO to avoid FutureWarning.
    - Scans all tables to find one that has a 'Date' column.
    - Handles both old-style and new descriptive column names like:
      'Close Close price adjusted for splits.'
      'Adj Close Adjusted close price adjusted for splits and dividend...'
    """
    tables = pd.read_html(StringIO(html))
    if not tables:
        raise RuntimeError(f"No tables found in price HTML for {ticker}")

    # Find the first table that has a Date column
    df = None
    for t in tables:
        if "Date" in t.columns:
            df = t.copy()
            break

    if df is None:
        raise RuntimeError(
            f"No suitable price table with 'Date' column found for {ticker}. "
            f"Found tables with columns: {[list(t.columns) for t in tables]}"
        )

    cols = list(df.columns)

    # Identify close & adj close columns by prefix
    close_candidates = [c for c in cols if str(c).startswith("Close")]
    adj_candidates = [c for c in cols if str(c).startswith("Adj Close")]

    if not close_candidates or not adj_candidates:
        raise RuntimeError(
            f"Unexpected columns in price table for {ticker}: {cols}"
        )

    close_col = close_candidates[0]
    adj_col = adj_candidates[0]

    # Drop rows that are "Dividend" events or non-price rows
    df = df[~df[close_col].astype(str).str.contains("Dividend", na=False)]
    df = df[df["Date"].astype(str).str.contains(r"\d{4}", na=False)]

    # Rename columns to a clean schema
    rename_map = {
        close_col: "Close",
        adj_col: "Adj_Close",
    }

    # Some layouts label Open as "Open*" or similar
    if "Open*" in df.columns:
        rename_map["Open*"] = "Open"
    # If "Open" is already there, no need to rename

    df = df.rename(columns=rename_map)

    # Ensure required columns exist
    for col in ["Open", "High", "Low", "Close", "Adj_Close", "Volume"]:
        if col not in df.columns:
            df[col] = pd.NA

    # Type conversions
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

    for col in ["Open", "High", "Low", "Close", "Adj_Close"]:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.replace("-", "", regex=False)
            .replace("", pd.NA)
        )
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["Volume"] = (
        df["Volume"]
        .astype(str)
        .str.replace(",", "", regex=False)
        .replace("-", "0")
    )
    df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce").fillna(0)

    df = df.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)

    return df[["Date", "Open", "High", "Low", "Close", "Adj_Close", "Volume"]]


def parse_yahoo_dividends_table(html: str, ticker: str) -> pd.DataFrame:
    """
    Parse dividends table from Yahoo HTML into a DataFrame.

    Typical columns:
        Date, Dividends

    We will clean it to:
        Date, Dividend
    """
    tables = pd.read_html(StringIO(html))
    if not tables:
        # It's possible the ticker never paid dividends
        return pd.DataFrame(columns=["Date", "Dividend"])

    df = tables[0].copy()

    if "Date" not in df.columns:
        return pd.DataFrame(columns=["Date", "Dividend"])

    # Usually second column contains "0.26 Dividend" etc.
    if df.shape[1] >= 2:
        value_col = df.columns[1]
    else:
        return pd.DataFrame(columns=["Date", "Dividend"])

    # Keep rows that mention 'Dividend'
    df = df[df[value_col].astype(str).str.contains("Dividend", na=False)]

    def extract_dividend(x: str) -> Optional[float]:
        parts = str(x).split()
        for p in parts:
            try:
                return float(p.replace(",", ""))
            except ValueError:
                continue
        return None

    df["Dividend"] = df[value_col].apply(extract_dividend)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date", "Dividend"]).reset_index(drop=True)

    return df[["Date", "Dividend"]]


# -----------------------------
# FETCH FUNCTIONS
# -----------------------------

def fetch_yahoo_prices(
    driver: webdriver.Chrome,
    ticker: str,
    max_retries: int = 3,
    delay_seconds: float = BASE_DELAY_SECONDS,
) -> pd.DataFrame:
    """
    Use Selenium to get full price history HTML and parse into a DataFrame.
    Retries a few times if Yahoo returns a weird/temporary layout.
    """
    url = yahoo_history_url(ticker)
    last_error = None

    for attempt in range(1, max_retries + 1):
        print(f"[PRICE] {ticker} (attempt {attempt}/{max_retries}) -> {url}")
        driver.get(url)
        time.sleep(delay_seconds)

        html = driver.page_source
        try:
            df = parse_yahoo_price_table(html, ticker)
            return df
        except Exception as e:
            last_error = e
            print(f"[WARN] {ticker}: price parse failed on attempt {attempt}: {e}")
            time.sleep(delay_seconds + 2.0)

    raise RuntimeError(
        f"Failed to fetch/parse price history for {ticker} after {max_retries} attempts. "
        f"Last error: {last_error}"
    )


def fetch_yahoo_dividends(
    driver: webdriver.Chrome,
    ticker: str,
    delay_seconds: float = BASE_DELAY_SECONDS,
) -> pd.DataFrame:
    """
    Use Selenium to get dividends HTML and parse into a DataFrame.
    """
    url = yahoo_dividends_url(ticker)
    print(f"[DIV]   {ticker} -> {url}")
    driver.get(url)
    time.sleep(delay_seconds)

    html = driver.page_source
    df = parse_yahoo_dividends_table(html, ticker)
    return df


# -----------------------------
# SAVE & MERGE HELPERS
# -----------------------------

def save_dataframe(df: pd.DataFrame, ticker: str, base_dir: str, suffix: str) -> str:
    """
    Save DataFrame to CSV under a base directory.
    """
    os.makedirs(base_dir, exist_ok=True)
    filename = f"{ticker.replace('/', '-')}_{suffix}.csv"
    path = os.path.join(base_dir, filename)
    df.to_csv(path, index=False)
    print(f"[SAVE] {ticker}: {path}")
    return path


def merge_price_and_dividends(price_df: pd.DataFrame, div_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge price history with dividends into a single frame:

        Date, Open, High, Low, Close, Adj_Close, Volume, Dividend

    Dividend is 0.0 on non-dividend dates.
    """
    df = price_df.copy()

    if div_df is None or div_df.empty:
        df["Dividend"] = 0.0
        return df

    df["Date"] = df["Date"].dt.normalize()
    div_df = div_df.copy()
    div_df["Date"] = div_df["Date"].dt.normalize()

    df = df.merge(div_df, on="Date", how="left")
    df["Dividend"] = df["Dividend"].fillna(0.0)

    return df


# -----------------------------
# UNIVERSE / ORCHESTRATION
# -----------------------------

def load_universe(path: str) -> List[str]:
    """
    Load tickers from a universe CSV.

    Supports:
    - 'Ticker' column (df_zacks style)
    - 'ticker' column
    - or a single-column CSV of tickers
    """
    df = pd.read_csv(path)

    if "Ticker" in df.columns:
        col = "Ticker"
    elif "ticker" in df.columns:
        col = "ticker"
    else:
        col = df.columns[0]

    tickers = (
        df[col]
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .unique()
        .tolist()
    )
    return tickers


def combined_file_exists(ticker: str) -> bool:
    """
    Check if we already have a combined Yahoo file for the ticker.
    Used for resume-safe behavior.
    """
    os.makedirs(DATA_DIR_COMBINED, exist_ok=True)
    fname = f"{ticker.replace('/', '-')}_combined.csv"
    path = os.path.join(DATA_DIR_COMBINED, fname)
    return os.path.exists(path)


def fetch_one_ticker_yahoo(driver: webdriver.Chrome, ticker: str) -> None:
    """
    Fetch price + dividends for a single ticker, merge, and save CSVs.
    Resume-safe: if combined CSV already exists, it skips the ticker.
    """
    ticker = ticker.strip()

    if combined_file_exists(ticker):
        print(f"[SKIP] {ticker}: combined file already exists.")
        return

    try:
        # Prices
        price_df = fetch_yahoo_prices(driver, ticker)
        save_dataframe(price_df, ticker, DATA_DIR_PRICES, "prices")

        # Dividends
        div_df = fetch_yahoo_dividends(driver, ticker)
        if not div_df.empty:
            save_dataframe(div_df, ticker, DATA_DIR_DIVS, "dividends")
        else:
            print(f"[INFO] {ticker}: No dividends found.")

        # Merge
        combined = merge_price_and_dividends(price_df, div_df)
        save_dataframe(combined, ticker, DATA_DIR_COMBINED, "combined")

        # Small extra delay between tickers
        time.sleep(EXTRA_DELAY_SECONDS)

    except Exception as e:
        print(f"[ERROR] {ticker}: {e}")


def fetch_universe_yahoo(universe_df: pd.DataFrame, enable_long_pauses: bool = True) -> None:
    """
    Main Yahoo fetch loop over the universe DataFrame.
    Expects a column 'ticker' in universe_df.

    If enable_long_pauses is False, it will NOT sleep 2–5 minutes every
    PAUSE_EVERY_N_TICKERS; it will still respect per-page delays.
    """
    driver = build_driver()
    try:
        total = len(universe_df)
        for i, row in universe_df.iterrows():
            idx = i + 1
            ticker = str(row["ticker"]).strip()

            print(f"\n[UNIVERSE] ({idx}/{total}) {ticker}")
            fetch_one_ticker_yahoo(driver, ticker)

            # Long pause every N tickers to be polite to Yahoo
            if enable_long_pauses and idx % PAUSE_EVERY_N_TICKERS == 0 and idx < total:
                pause = random.uniform(PAUSE_MIN_SECONDS, PAUSE_MAX_SECONDS)
                print(f"[PAUSE] Processed {idx} tickers. Sleeping for ~{pause/60:.1f} minutes...")
                time.sleep(pause)

    finally:
        print("[INFO] Selenium driver closed.")
        driver.quit()


def main():
    args = parse_args()
    tickers = load_universe(args.universe)
    print(f"[INFO] Universe size: {len(tickers)} tickers")

    universe_df = pd.DataFrame({"ticker": tickers})

    print("\n[STEP] Fetch Yahoo prices + dividends for universe\n")

    enable_long_pauses = not args.no_long_pauses
    fetch_universe_yahoo(universe_df, enable_long_pauses=enable_long_pauses)


if __name__ == "__main__":
    main()



import os
import time
import requests
import pandas as pd
from pathlib import Path

# -----------------------------
# Configuration
# -----------------------------
BENCHMARK_TICKERS = ["VOO", "VTV", "IVE", "VIG", "SCHD"]

ROOT = Path(__file__).resolve().parents[1]   # points to VinV_1_0/
DATA_DIR = ROOT / "data_yahoo_combined"
DATA_DIR.mkdir(exist_ok=True)

def yahoo_price_url(ticker):
    return (
        f"https://query1.finance.yahoo.com/v7/finance/download/{ticker}"
        f"?period1=0&period2=9999999999&interval=1d&events=history"
        f"&includeAdjustedClose=true"
    )

def yahoo_div_url(ticker):
    return (
        f"https://query1.finance.yahoo.com/v7/finance/download/{ticker}"
        f"?period1=0&period2=9999999999&interval=1d&events=div"
    )

def fetch_csv(url, label, sleeps=2):
    for i in range(5):
        try:
            r = requests.get(url, timeout=20)
            if r.status_code == 200:
                from io import StringIO
                return pd.read_csv(StringIO(r.text))
            print(f"[WARN] {label} HTTP {r.status_code}, retry {i+1}/5")
        except Exception as e:
            print(f"[ERR] {label} {e}, retrying...")
        time.sleep(sleeps * (i+1))
    print(f"[FAIL] {label} failed after 5 attempts")
    return pd.DataFrame()

def process_combined(df_p, df_d, ticker):
    df_p = df_p.rename(columns={"Date": "date", "Adj Close": "price"})
    df_p["ticker"] = ticker
    df_p = df_p[["date", "ticker", "price"]]

    if not df_d.empty and "Dividends" in df_d.columns:
        df_d = df_d.rename(columns={"Date": "date", "Dividends": "dividend"})
        df = df_p.merge(df_d[["date", "dividend"]], on="date", how="left")
    else:
        df_p["dividend"] = 0.0
        df = df_p

    return df

def main():
    print(f"[INFO] Saving ETF benchmark data to: {DATA_DIR}")
    print(f"[INFO] Benchmark tickers: {BENCHMARK_TICKERS}")

    for ticker in BENCHMARK_TICKERS:
        print(f"\n[FETCH] {ticker}")

        url_p = yahoo_price_url(ticker)
        url_d = yahoo_div_url(ticker)

        print(f"[PRICE] {url_p}")
        df_p = fetch_csv(url_p, f"{ticker} price")

        print(f"[DIV]   {url_d}")
        df_d = fetch_csv(url_d, f"{ticker} div")

        combined = process_combined(df_p, df_d, ticker)
        outfile = DATA_DIR / f"{ticker}_combined.csv"
        combined.to_csv(outfile, index=False)

        print(f"[SAVE] {ticker} -> {outfile}")

if __name__ == "__main__":
    main()

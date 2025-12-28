"""
analytics/make_panel_prices_dividends.py

Build a panel file:
    date, ticker, price, dividend

from per-ticker combined Yahoo files in:
    data_yahoo_combined/<TICKER>_combined.csv

Outputs:
    panel_prices_dividends.parquet
    panel_prices_dividends.csv.gz
"""

import os
import pandas as pd

# Paths are relative to the directory where you RUN Python (ViV_t)
UNIVERSE_FILE = "df_zacks.csv"
COMBINED_DIR = "data_yahoo_combined"

PANEL_PARQUET = "panel_prices_dividends.parquet"
PANEL_CSV_GZ = "panel_prices_dividends.csv.gz"


def load_universe(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Universe file not found: {path}")

    df = pd.read_csv(path)
    if "Ticker" not in df.columns:
        raise RuntimeError("df_zacks.csv must contain a 'Ticker' column.")

    df["Ticker"] = df["Ticker"].astype(str).str.strip()
    df = df[df["Ticker"] != ""].reset_index(drop=True)
    return df


def load_combined_for_ticker(ticker: str) -> pd.DataFrame:
    """
    Load per-ticker combined Yahoo file and reduce to:
        date, ticker, price, dividend
    """
    fname = f"{ticker.replace('/', '-')}_combined.csv"
    path = os.path.join(COMBINED_DIR, fname)
    if not os.path.exists(path):
        return pd.DataFrame(columns=["date", "ticker", "price", "dividend"])

    df = pd.read_csv(path)

    if "Date" not in df.columns:
        return pd.DataFrame(columns=["date", "ticker", "price", "dividend"])

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)

    # Choose adjusted close as the canonical price
    price_col = None
    for cand in ["Adj_Close", "Adj Close", "Close"]:
        if cand in df.columns:
            price_col = cand
            break

    if price_col is None:
        return pd.DataFrame(columns=["date", "ticker", "price", "dividend"])

    # Ensure Dividend column exists (0 if missing)
    if "Dividend" not in df.columns:
        df["Dividend"] = 0.0

    out = pd.DataFrame(
        {
            "date": df["Date"],
            "ticker": ticker,
            "price": df[price_col].astype(float),
            "dividend": df["Dividend"].astype(float).fillna(0.0),
        }
    )

    return out


def main():
    print("[STEP] Loading df_zacks.csv")
    universe = load_universe(UNIVERSE_FILE)
    print(f"[INFO] Universe size: {len(universe)} tickers")

    panel_chunks = []

    for i, row in universe.iterrows():
        ticker = row["Ticker"]
        print(f"[TICKER] ({i+1}/{len(universe)}) {ticker}")

        df_t = load_combined_for_ticker(ticker)
        if df_t.empty:
            print(f"[WARN] No combined data found for {ticker}, skipping.")
            continue

        panel_chunks.append(df_t)

    if not panel_chunks:
        raise RuntimeError("No panel data constructed; all tickers empty?")

    panel = pd.concat(panel_chunks, axis=0, ignore_index=True)
    panel = panel.sort_values(["date", "ticker"]).reset_index(drop=True)

    # Save in two formats: Parquet + compressed CSV
    panel.to_parquet(PANEL_PARQUET, index=False)
    panel.to_csv(PANEL_CSV_GZ, index=False, compression="gzip")

    print("[INFO] Saved panel to:")
    print(f"  {PANEL_PARQUET}")
    print(f"  {PANEL_CSV_GZ}")
    print("[DONE]")


if __name__ == "__main__":
    main()


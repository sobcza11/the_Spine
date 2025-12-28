# analytics/make_missing_universe.py

import os
import pandas as pd

UNIVERSE_FILE = "df_zacks.csv"
COMBINED_DIR = "data_yahoo_combined"
MISSING_OUT = "df_zacks_missing.csv"

def main():
    # Load master universe
    df = pd.read_csv(UNIVERSE_FILE)
    if "Ticker" in df.columns:
        col = "Ticker"
    elif "ticker" in df.columns:
        col = "ticker"
    else:
        col = df.columns[0]

    df[col] = df[col].astype(str).str.strip()
    df = df[df[col] != ""]
    universe = df[col].unique().tolist()

    # Existing combined tickers
    os.makedirs(COMBINED_DIR, exist_ok=True)
    combined_files = [
        f for f in os.listdir(COMBINED_DIR)
        if f.endswith("_combined.csv")
    ]
    combined_tickers = {
        f.replace("_combined.csv", "") for f in combined_files
    }

    # Normalize ticker → file-name form (we used replace("/", "-"))
    def norm(t):
        return t.replace("/", "-")

    universe_norm = [norm(t) for t in universe]

    missing_mask = [norm(t) not in combined_tickers for t in df[col]]
    df_missing = df[missing_mask].copy()

    print(f"Total universe tickers: {len(universe)}")
    print(f"Have combined files  : {len(combined_tickers)}")
    print(f"Missing combined     : {df_missing[col].nunique()}")

    df_missing.to_csv(MISSING_OUT, index=False)
    print(f"[SAVE] Missing universe -> {MISSING_OUT}")

if __name__ == "__main__":
    main()

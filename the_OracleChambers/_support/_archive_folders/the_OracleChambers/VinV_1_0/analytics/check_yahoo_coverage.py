# analytics/check_yahoo_coverage.py

import os
import pandas as pd

UNIVERSE_FILE = "df_zacks.csv"
COMBINED_DIR = "data_yahoo_combined"

def main():
    # Load universe
    df = pd.read_csv(UNIVERSE_FILE)
    if "Ticker" in df.columns:
        col = "Ticker"
    elif "ticker" in df.columns:
        col = "ticker"
    else:
        col = df.columns[0]

    universe = (
        df[col]
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .unique()
        .tolist()
    )

    # Combined files present
    os.makedirs(COMBINED_DIR, exist_ok=True)
    combined_files = os.listdir(COMBINED_DIR)
    combined_tickers = {
        f.split("_combined.csv")[0] for f in combined_files
        if f.endswith("_combined.csv")
    }

    # Normalize universe tickers the same way
    def norm(t):
        return t.replace("/", "-")

    universe_norm = [norm(t) for t in universe]

    done = [t for t in universe_norm if t in combined_tickers]
    missing = [t for t in universe_norm if t not in combined_tickers]

    total = len(universe_norm)
    done_n = len(done)
    missing_n = len(missing)

    print(f"Total universe tickers: {total}")
    print(f"Have combined files  : {done_n}")
    print(f"Missing combined     : {missing_n}")
    if total > 0:
        pct = done_n * 100.0 / total
        print(f"Coverage             : {pct:.2f}%")

    # Optional: show first 20 missing
    print("\nSample missing tickers (up to 20):")
    for t in missing[:20]:
        print("  ", t)

if __name__ == "__main__":
    main()


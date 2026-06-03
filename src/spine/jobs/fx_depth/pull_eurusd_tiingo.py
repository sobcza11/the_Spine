from pathlib import Path
import os
import requests
import pandas as pd

REPO_ROOT = Path.cwd()
OUT = REPO_ROOT / "data" / "fx" / "fx_depth" / "raw" / "eurusd.parquet"

SYMBOL = "eurusd"
START_DATE = "2020-01-01"

def main():
    api_key = os.getenv("TIINGO_API_KEY")
    if not api_key:
        raise RuntimeError("Missing TIINGO_API_KEY environment variable.")

    OUT.parent.mkdir(parents=True, exist_ok=True)

    url = f"https://api.tiingo.com/tiingo/fx/{SYMBOL}/prices"
    params = {
        "startDate": START_DATE,
        "resampleFreq": "1day",
        "token": api_key
    }

    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()

    rows = r.json()
    if not rows:
        raise RuntimeError("Tiingo returned no EUR/USD rows.")

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
    df["value"] = pd.to_numeric(df["close"], errors="coerce")

    df = (
        df[["date", "value"]]
        .dropna()
        .sort_values("date")
        .reset_index(drop=True)
    )

    df.to_parquet(OUT, index=False)

    print(f"BUILT: {OUT}")
    print(f"ROWS: {len(df)}")
    print(f"AS OF: {df['date'].max().date()}")
    print(f"LATEST EUR/USD: {df['value'].iloc[-1]:.5f}")

if __name__ == "__main__":
    main()

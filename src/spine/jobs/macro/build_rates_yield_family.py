import requests
import pandas as pd
from pathlib import Path
import os

FRED_API_KEY = "f41c1162e50de7362129a5c052dc1327"  # <- set in .env later
if not FRED_API_KEY:
    raise RuntimeError("Missing FRED_API_KEY environment variable.")

START_DATE = "1990-01-01"

SERIES = {
    "us_2y_yield": "DGS2",
    "us_5y_yield": "DGS5",
    "us_10y_yield": "DGS10",
    "us_20y_yield": "DGS20",
    "us_30y_yield": "DGS30",
}

OUT_DIR = Path("data/macro/serving/rates")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def fetch_fred_series(series_id):
    url = "https://api.stlouisfed.org/fred/series/observations"

    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": START_DATE
    }

    for attempt in range(1, 4):
        r = requests.get(url, params=params, timeout=30)

        if r.status_code == 200:
            break

        print(f"FRED retry {attempt}/3 | {series_id} | HTTP {r.status_code}")

        if attempt == 3:
            print(r.text[:500])
            r.raise_for_status()

    data = r.json()["observations"]

    df = pd.DataFrame(data)
    df = df[["date", "value"]]

    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    return df


def fetch_all():
    dfs = []

    for name, code in SERIES.items():
        df = fetch_fred_series(code)
        df = df.rename(columns={"value": name})
        dfs.append(df.set_index("date"))

    combined = pd.concat(dfs, axis=1)
    combined = combined.reset_index().sort_values("date")

    return combined


def add_spreads(df):
    df["us_10y_2y_spread"] = df["us_10y_yield"] - df["us_2y_yield"]
    df["us_30y_5y_spread"] = df["us_30y_yield"] - df["us_5y_yield"]
    df["us_20y_10y_spread"] = df["us_20y_yield"] - df["us_10y_yield"]
    df["us_30y_10y_spread"] = df["us_30y_yield"] - df["us_10y_yield"]
    return df


def main():
    df = fetch_all()
    df = add_spreads(df)

    panel_path = OUT_DIR / "rates_us_yield_family.parquet"
    json_path = OUT_DIR / "rates_us_yield_family.json"
    latest_path = OUT_DIR / "rates_us_yield_family_latest.json"

    df.to_parquet(panel_path, index=False)

    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    df.to_json(json_path, orient="records", indent=2)

    df.tail(1).to_json(latest_path, orient="records", indent=2)

    print("PASS")
    print("Built US Yield Family")


if __name__ == "__main__":
    main()  

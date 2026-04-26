import os
import pandas as pd
import requests

FRED_API_KEY = os.getenv("FRED_API_KEY")

SERIES_ID = "RBCNBIS"
SYMBOL = "CN10Y_PROXY"

URL = "https://api.stlouisfed.org/fred/series/observations"


def fetch_series():
    params = {
        "series_id": SERIES_ID,
        "api_key": FRED_API_KEY,
        "file_type": "json"
    }

    r = requests.get(URL, params=params, timeout=30)
    r.raise_for_status()

    data = r.json()["observations"]

    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    df = df.dropna().sort_values("date").reset_index(drop=True)

    return df


def main():
    df = fetch_series()

    df_out = df[["date", "value"]]
    df_out = df_out.rename(columns={"value": "y10_proxy"})

    out_path = "data/serving/rates/china/china_y10_proxy.json"
    df_out.to_json(out_path, orient="records", date_format="iso")

    print(f"Saved to {out_path}")


if __name__ == "__main__":
    main()

    
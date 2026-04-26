import os
import pandas as pd
import requests

FRED_API_KEY = os.getenv("FRED_API_KEY")

SERIES_ID = "INTDSRCNM193N"
SYMBOL = "CN_POLICY"

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


def compute_policy_pressure(df):
    df["policy_z"] = (df["value"] - df["value"].rolling(252).mean()) / df["value"].rolling(252).std()

    df["CN_policy_pressure_t1"] = df["policy_z"]

    df["state"] = pd.cut(
        df["CN_policy_pressure_t1"],
        bins=[-999, -1, 1, 999],
        labels=["Low", "Moderate", "High"]
    )

    return df


def main():
    df = fetch_series()
    df = compute_policy_pressure(df)

    df_out = df[["date", "value", "policy_z", "CN_policy_pressure_t1", "state"]]

    out_path = "data/serving/rates/china/china_policy.json"
    df_out.to_json(out_path, orient="records", date_format="iso")

    print(f"Saved to {out_path}")


if __name__ == "__main__":
    main()

    
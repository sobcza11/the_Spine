from pathlib import Path
import pandas as pd
import requests
import os

OUT = Path("data/fx/fx_depth/raw/natgas.parquet")

API_KEY = os.environ["EIA_API_KEY"]

url = (
    "https://api.eia.gov/v2/natural-gas/pri/fut/data/"
)

params = {
    "api_key": API_KEY,
    "frequency": "daily",
    "data[0]": "value",
    "sort[0][column]": "period",
    "sort[0][direction]": "desc",
    "length": 5000
}

r = requests.get(url, params=params, timeout=60)
r.raise_for_status()

rows = r.json()["response"]["data"]

df = pd.DataFrame(rows)

df["date"] = pd.to_datetime(df["period"])
df["value"] = pd.to_numeric(df["value"])

df = df[["date", "value"]].sort_values("date")

OUT.parent.mkdir(parents=True, exist_ok=True)
df.to_parquet(OUT, index=False)

print(f"SAVED {OUT} rows={len(df)}")


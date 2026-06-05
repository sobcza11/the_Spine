from pathlib import Path
import os
import requests
import pandas as pd

REPO_ROOT = Path.cwd()
OUT = REPO_ROOT / "data" / "fx" / "fx_depth" / "raw" / "brent.parquet"

EIA_API_KEY = os.environ["EIA_API_KEY"]

URL = "https://api.eia.gov/v2/petroleum/pri/spt/data/"

PARAMS = {
    "api_key": EIA_API_KEY,
    "frequency": "daily",
    "data[0]": "value",
    "facets[product][]": "EPCBRENT",
    "sort[0][column]": "period",
    "sort[0][direction]": "desc",
    "length": 5000,
}

r = requests.get(URL, params=PARAMS, timeout=60)
r.raise_for_status()

df = pd.DataFrame(r.json()["response"]["data"])

df["date"] = pd.to_datetime(df["period"])
df["value"] = pd.to_numeric(df["value"], errors="coerce")

df = df[["date", "value"]].dropna().sort_values("date")

OUT.parent.mkdir(parents=True, exist_ok=True)
df.to_parquet(OUT, index=False)

print(f"SAVED: Brent Crude | {OUT} | rows={len(df)} | as_of={df['date'].max().date()}")
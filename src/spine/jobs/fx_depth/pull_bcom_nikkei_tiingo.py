from pathlib import Path
import os
import requests
import pandas as pd

REPO_ROOT = Path.cwd()
OUT_DIR = REPO_ROOT / "data" / "fx" / "fx_depth" / "raw"

TOKEN = os.environ["TIINGO_API_KEY"]

def get_tiingo(symbol):
    url = f"https://api.tiingo.com/tiingo/daily/{symbol}/prices"

    r = requests.get(
        url,
        params={
            "startDate": "2015-01-01",
            "token": TOKEN,
            "format": "json",
        },
        timeout=60,
    )
    r.raise_for_status()

    df = pd.DataFrame(r.json())

    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df["value"] = pd.to_numeric(df["adjClose"], errors="coerce")

    return df[["date", "value"]].dropna()

bcom = get_tiingo("DBC")
nikkei = get_tiingo("EWJ")

OUT_DIR.mkdir(parents=True, exist_ok=True)

bcom.to_parquet(OUT_DIR / "bcom.parquet", index=False)
nikkei.to_parquet(OUT_DIR / "nikkei.parquet", index=False)

print(
    f"SAVED: BCOM proxy | DBC | rows={len(bcom)} | "
    f"as_of={bcom['date'].max().date()}"
)

print(
    f"SAVED: Nikkei proxy | EWJ | rows={len(nikkei)} | "
    f"as_of={nikkei['date'].max().date()}"
)


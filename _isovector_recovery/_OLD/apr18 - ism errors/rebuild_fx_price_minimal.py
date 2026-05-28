import requests
import pandas as pd
from pathlib import Path

API_KEY = "21681caa5a11c7eac5d2fb5ca66a02ae22966861"

PAIRS = {
    "EURUSD": "eurusd",
    "AUDUSD": "audusd",
    "GBPUSD": "gbpusd",
    "USDJPY": "usdjpy",
    "USDCAD": "usdcad",
    "USDCHF": "usdchf",
}

OUT_PATH = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\spine_us\us_fx_spot_cross_t2.parquet")

all_rows = []

for sym, tiingo_sym in PAIRS.items():
    url = f"https://api.tiingo.com/tiingo/fx/{tiingo_sym}/prices"
    headers = {"Authorization": f"Token {API_KEY}"}
    params = {
    "resampleFreq": "1day",
    "startDate": "2020-01-01"
}

    r = requests.get(url, params={**params, "token": API_KEY})
    r.raise_for_status()

    params = {  "resampleFreq": "1day",
                "startDate": "2020-01-01"}

    r = requests.get(url, headers=headers, params=params)
    r.raise_for_status()

    df = pd.DataFrame(r.json())

    df = df.rename(columns={
        "date": "date",
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close"
    })

    df["symbol"] = sym
    df["date"] = pd.to_datetime(df["date"])

    all_rows.append(df[["symbol", "date", "open", "high", "low", "close"]])

final_df = pd.concat(all_rows).sort_values(["symbol", "date"])

OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
final_df.to_parquet(OUT_PATH, index=False)

print("Wrote:", OUT_PATH)
print("Symbols:", sorted(final_df["symbol"].unique()))
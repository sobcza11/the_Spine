from pathlib import Path
import pandas as pd
from fredapi import Fred

FRED_API_KEY = "f41c1162e50de7362129a5c052dc1327"

fred = Fred(api_key=FRED_API_KEY)

SERIES = {
    "us_2y": "DGS2",
    "de_2y": "IR3TIB01DEM156N",
    "uk_2y": "IR3TIB01GBM156N",
    "ca_2y": "IR3TIB01CAM156N",
    "jp_2y": "IR3TIB01JPM156N",
    "ch_2y": "IR3TIB01CHM156N",
    "gold": "GOLDAMGBD228NLBM",
    "copper": "PCOPPUSDM",
    "iron_ore": "PIORECRUSDM",
    "wti": "DCOILWTICO",
    "natgas": "DHHNGSP",
    "vix": "VIXCLS",
    "us_10y_real": "DFII10"
}

OUT_DIR = Path.cwd() / "data" / "fx" / "fx_depth" / "raw"
OUT_DIR.mkdir(parents=True, exist_ok=True)

all_data = {}

for name, series_id in SERIES.items():
    try:
        s = fred.get_series(series_id)

        df = (
            s.rename("value")
             .reset_index()
             .rename(columns={"index": "date"})
        )

        df["date"] = pd.to_datetime(df["date"])
        df["series_id"] = series_id

        out_file = OUT_DIR / f"{name}.parquet"
        df.to_parquet(out_file, index=False)

        all_data[name] = df

        print(f"OK  {name:<15} {len(df):>8,} rows -> {out_file}")

    except Exception as e:
        print(f"FAIL {name}: {e}")

print("")
print("=" * 80)
print("HEAD(3)")
print("=" * 80)

for name, df in all_data.items():
    print("")
    print(name.upper())
    print(df.head(3))

print("")
print("=" * 80)
print("TAIL(3)")
print("=" * 80)

for name, df in all_data.items():
    print("")
    print(name.upper())
    print(df.tail(3))

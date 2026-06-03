from pathlib import Path
import json
import pandas as pd

ROOT = Path.cwd()

INPUTS = {
    "EUR/USD": {
        "metric": "DE-US 2Y",
        "path": ROOT / "data" / "fx" / "fx_depth" / "processed" / "eurusd" / "de_us_2y_spread.parquet",
        "value_col": "de_us_2y_spread",
        "change_col": "de_us_2y_spread_dod"
    },
    "USD/CAD": {
        "metric": "US-CA 2Y",
        "path": ROOT / "data" / "fx" / "fx_depth" / "processed" / "usdcad" / "us_ca_2y_spread.parquet",
        "value_col": "us_ca_2y_spread",
        "change_col": "us_ca_2y_spread_dod"
    }
}

OUT_DIR = ROOT / "data" / "serving" / "fx"
OUT_DIR.mkdir(parents=True, exist_ok=True)

payload = {
    "meta": {
        "dataset": "fx_depth_serving_v1",
        "source": "the_Spine | FX Depth | Official Rates Sources",
        "frequency": "weekday_where_available",
        "change_metric": "DoD"
    },
    "pairs": {}
}

for pair, cfg in INPUTS.items():
    df = pd.read_parquet(cfg["path"]).copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.dropna(subset=[cfg["value_col"]]).sort_values("date")

    rows = []

    for _, row in df.tail(45).iterrows():
        rows.append({
            "date": row["date"].strftime("%Y-%m-%d"),
            "metric": cfg["metric"],
            "value": round(float(row[cfg["value_col"]]), 4),
            "change": (
                round(float(row[cfg["change_col"]]), 4)
                if pd.notna(row.get(cfg["change_col"]))
                else None
            )
        })

    payload["pairs"][pair] = {
        "metric": cfg["metric"],
        "latest": rows[-1] if rows else None,
        "rows": rows
    }

out_file = OUT_DIR / "fx_depth_serving_v1.json"

with open(out_file, "w", encoding="utf-8") as f:
    json.dump(payload, f, indent=2)

print("")
print("=" * 80)
print("FX DEPTH SERVING CREATED")
print("=" * 80)
print(out_file)

print("")
print("=" * 80)
print("EUR/USD .tail(3)")
print("=" * 80)
print(pd.DataFrame(payload["pairs"]["EUR/USD"]["rows"]).tail(3))

print("")
print("=" * 80)
print("USD/CAD .tail(3)")
print("=" * 80)
print(pd.DataFrame(payload["pairs"]["USD/CAD"]["rows"]).tail(3))

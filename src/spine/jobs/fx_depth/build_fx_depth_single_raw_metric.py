# src\spine\jobs\fx_depth\build_fx_depth_single_raw_metric.py
from pathlib import Path
import json
import pandas as pd

REPO_ROOT = Path.cwd()

INPUTS = {
    "USD/JPY": {
        "US 2Y": REPO_ROOT / "data" / "fx" / "fx_depth" / "raw" / "us_2y.parquet"
    },
    "USD/CHF": {
        "VIX": REPO_ROOT / "data" / "fx" / "fx_depth" / "raw" / "vix.parquet"
    },
    "AUD/USD": {
        "Iron Ore": REPO_ROOT / "data" / "fx" / "fx_depth" / "raw" / "iron_ore.parquet"
    }
}

OUT = REPO_ROOT / "data" / "serving" / "fx" / "fx_depth_serving_v1.json"


def pick_value_col(df):
    for col in ["value", "close", "price", "yield", "rate", "PX_LAST"]:
        if col in df.columns:
            return col
    numeric = df.select_dtypes("number").columns.tolist()
    if not numeric:
        raise ValueError(f"No numeric value column found. Columns: {list(df.columns)}")
    return numeric[0]


def build_rows(path):
    df = pd.read_parquet(path).copy()

    if "date" not in df.columns:
        raise KeyError(f"{path} missing date column. Found: {list(df.columns)}")

    df["date"] = pd.to_datetime(df["date"])
    value_col = pick_value_col(df)

    df = df[["date", value_col]].dropna().sort_values("date")
    df["change"] = df[value_col].diff()

    return [
        {
            "date": r["date"].strftime("%Y-%m-%d"),
            "value": round(float(r[value_col]), 4),
            "change": round(float(r["change"]), 4) if pd.notna(r["change"]) else 0.0
        }
        for _, r in df.iterrows()
    ]


def main():
    payload = json.loads(OUT.read_text(encoding="utf-8")) if OUT.exists() else {
        "source": "the_Spine | FX DEPTH",
        "pairs": {}
    }

    payload.setdefault("pairs", {})

    for pair, metrics in INPUTS.items():
        pair_payload = payload["pairs"].setdefault(pair, {
            "source": f"Source: the_Spine | FX DEPTH | {pair}",
            "metrics": {}
        })

        pair_payload.setdefault("metrics", {})

        for metric, path in metrics.items():
            rows = build_rows(path)
            pair_payload["metrics"][metric] = {
                "metric": metric,
                "source": f"Source: the_Spine | FX DEPTH | {metric}",
                "as_of_date": rows[-1]["date"] if rows else None,
                "rows": rows
            }

            print(f"BUILT: {pair} | {metric} | rows={len(rows)} | as_of={rows[-1]['date'] if rows else '--'}")

    OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"MERGED INTO: {OUT}")


if __name__ == "__main__":
    main()
    
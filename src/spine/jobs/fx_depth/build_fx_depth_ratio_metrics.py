# src/spine/jobs/fx_depth/build_fx_depth_ratio_metrics.py

from pathlib import Path
import json
import pandas as pd

REPO_ROOT = Path.cwd()

OUT = REPO_ROOT / "data" / "serving" / "fx" / "fx_depth_serving_v1.json"

CONFIG = [
    {
        "pair": "USD/CHF",
        "metric": "XAU/EUR",
        "left": REPO_ROOT / "data" / "fx" / "fx_depth" / "raw" / "gold.parquet",
        "right": REPO_ROOT / "data" / "fx" / "fx_depth" / "raw" / "eurusd.parquet",
        "method": "XAU/USD divided by EUR/USD",
    },
    {
        "pair": "AUD/USD",
        "metric": "Copper/Gold",
        "left": REPO_ROOT / "data" / "fx" / "fx_depth" / "raw" / "copper.parquet",
        "right": REPO_ROOT / "data" / "fx" / "fx_depth" / "raw" / "gold.parquet",
        "method": "Copper divided by Gold",
    },
    {
        "pair": "USD/CAD",
        "metric": "WTI vs. NatGas",
        "left": REPO_ROOT / "data" / "fx" / "fx_depth" / "raw" / "wti.parquet",
        "right": REPO_ROOT / "data" / "fx" / "fx_depth" / "raw" / "natgas.parquet",
        "method": "WTI divided by Henry Hub Natural Gas",
    },
    {
        "pair": "GBP/USD",
        "metric": "FTSE vs. SPX",
        "left": REPO_ROOT / "data" / "fx" / "fx_depth" / "raw" / "ftse_proxy.parquet",
        "right": REPO_ROOT / "data" / "fx" / "fx_depth" / "raw" / "spx_proxy.parquet",
        "method": "EWU divided by SPY",
    },

]


def value_col(df):
    for col in ["value", "close", "price", "PX_LAST"]:
        if col in df.columns:
            return col

    nums = df.select_dtypes("number").columns.tolist()

    if not nums:
        raise ValueError(f"No numeric value column found: {list(df.columns)}")

    return nums[0]


def load_series(path):
    df = pd.read_parquet(path).copy()

    if "date" not in df.columns:
        raise KeyError(f"{path} missing date column. Found: {list(df.columns)}")

    col = value_col(df)
    df["date"] = pd.to_datetime(df["date"])

    return (
        df[["date", col]]
        .rename(columns={col: "value"})
        .dropna()
        .sort_values("date")
    )


def build_ratio_rows(left_path, right_path):
    left = load_series(left_path).rename(columns={"value": "left"})
    right = load_series(right_path).rename(columns={"value": "right"})

    df = left.merge(right, on="date", how="inner")
    df = df[df["right"] != 0].copy()

    df["value"] = df["left"] / df["right"]
    df["change"] = df["value"].diff()

    return [
        {
            "date": row["date"].strftime("%Y-%m-%d"),
            "value": round(float(row["value"]), 6),
            "change": round(float(row["change"]), 6)
            if pd.notna(row["change"])
            else 0.0,
        }
        for _, row in df.iterrows()
    ]


def main():
    payload = (
        json.loads(OUT.read_text(encoding="utf-8"))
        if OUT.exists()
        else {
            "source": "the_Spine | FX DEPTH",
            "pairs": {},
        }
    )

    payload.setdefault("pairs", {})

    for cfg in CONFIG:
        if not cfg["left"].exists() or not cfg["right"].exists():
            print(
                f"SKIP: {cfg['pair']} | {cfg['metric']} | "
                f"missing left={cfg['left'].exists()} right={cfg['right'].exists()}"
            )
            continue

        rows = build_ratio_rows(cfg["left"], cfg["right"])

        pair_payload = payload["pairs"].setdefault(
            cfg["pair"],
            {
                "source": f"Source: the_Spine | FX DEPTH | {cfg['pair']}",
                "metrics": {},
            },
        )

        pair_payload.setdefault("metrics", {})

        pair_payload["metrics"][cfg["metric"]] = {
            "metric": cfg["metric"],
            "source": f"Source: the_Spine | FX DEPTH | {cfg['metric']} | {cfg['method']}",
            "method": cfg["method"],
            "as_of_date": rows[-1]["date"] if rows else None,
            "rows": rows,
        }

        print(
            f"BUILT: {cfg['pair']} | {cfg['metric']} | "
            f"rows={len(rows)} | as_of={rows[-1]['date'] if rows else '--'}"
        )

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(f"MERGED INTO: {OUT}")


if __name__ == "__main__":
    main()


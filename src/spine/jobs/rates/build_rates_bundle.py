from pathlib import Path
import pandas as pd

REPO_ROOT = Path.cwd()

TIER1 = REPO_ROOT / "data/rates/tier1/rates_tier1_panel.parquet"
ZT_OUT = REPO_ROOT / "data/rates/zt/rates_core_zt.parquet"
SERVING_OUT = REPO_ROOT / "data/macro/serving/rates/rates_serving_panel.parquet"

CORE_RATES = [
    "y2", "y5", "y10", "y30",
    "spread_10_2", "spread_30_5",
    "eu_y10", "uk_y10",
    "us_eu_10y_spread", "us_uk_10y_spread",
]


def expanding_z(s: pd.Series) -> pd.Series:
    return (s - s.expanding().mean()) / s.expanding().std()


def main():
    df = pd.read_parquet(TIER1).copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    missing = [c for c in CORE_RATES if c not in df.columns]
    if missing:
        raise KeyError(f"Missing CORE_RATES columns: {missing}")

    for col in CORE_RATES:
        df[f"{col}_z"] = expanding_z(df[col])

    z_cols = [f"{c}_z" for c in CORE_RATES]
    df["rates_core_zt_raw"] = df[z_cols].mean(axis=1)

    df["rates_core_zt"] = expanding_z(df["rates_core_zt_raw"]).fillna(0)

    zt = df[["date", "rates_core_zt"]].copy()
    zt.to_parquet(ZT_OUT, index=False)

    LATEST_JSON = REPO_ROOT / "data/macro/serving/rates/rates_latest.json"

    latest = zt.tail(1).copy()
    latest["date"] = latest["date"].dt.strftime("%Y-%m-%d")
    latest.to_json(LATEST_JSON, orient="records", indent=2)

    serving = df[["date"] + CORE_RATES + ["rates_core_zt"]].copy()
    serving.to_parquet(SERVING_OUT, index=False)

    print("OK | RATES bundle built")
    print("ZT:", ZT_OUT)
    print("SERVING:", SERVING_OUT)
    print(zt.tail())


if __name__ == "__main__":
    main()


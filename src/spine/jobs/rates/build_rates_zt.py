from pathlib import Path

import numpy as np
import pandas as pd


INPUT_PATH = Path("data/rates/tier1/rates_tier1_panel.parquet")
OUT_DIR = Path("data/rates/zt")
SERVE_DIR = Path("data/rates/serving")

OUT_DIR.mkdir(parents=True, exist_ok=True)
SERVE_DIR.mkdir(parents=True, exist_ok=True)


SIGNALS = [
    "spread_10_2",
    "spread_30_5",
    "policy_gap_2y_effr",
    "real_y10",
    "term_premium_10y",
    "us_eu_10y_spread",
    "us_jp_10y_spread",
    "us_uk_10y_spread",
]


def rolling_z(series: pd.Series, window: int = 756) -> pd.Series:
    mean = series.rolling(window=window, min_periods=252).mean()
    std = series.rolling(window=window, min_periods=252).std()
    return (series - mean) / std


def squash_z(z: pd.Series) -> pd.Series:
    return np.tanh(z / 2.0)


def classify_state(score: float) -> str:
    if score >= 0.35:
        return "Tightening"
    if score <= -0.35:
        return "Easing"
    return "Neutral"


def main() -> None:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Missing input file: {INPUT_PATH}")

    df = pd.read_parquet(INPUT_PATH).copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    missing = [c for c in SIGNALS if c not in df.columns]
    if missing:
        raise KeyError(f"Missing required columns: {missing}")

    for col in SIGNALS:
        df[f"{col}_z"] = rolling_z(df[col])
        df[f"{col}_s"] = squash_z(df[f"{col}_z"])

    # Directional convention:
    # Positive Zₜ = tighter / more restrictive bond-market state.
    df["curve_component"] = (
        -0.55 * df["spread_10_2_s"] +
        -0.45 * df["spread_30_5_s"]
    )

    df["policy_component"] = df["policy_gap_2y_effr_s"]

    df["real_component"] = df["real_y10_s"]

    df["risk_component"] = df["term_premium_10y_s"]

    df["global_component"] = df[
        [
            "us_eu_10y_spread_s",
            "us_jp_10y_spread_s",
            "us_uk_10y_spread_s",
        ]
    ].mean(axis=1)

    df["rates_zt"] = (
        0.25 * df["curve_component"] +
        0.25 * df["policy_component"] +
        0.20 * df["real_component"] +
        0.15 * df["risk_component"] +
        0.15 * df["global_component"]
    )

    component_cols = [
        "curve_component",
        "policy_component",
        "real_component",
        "risk_component",
        "global_component",
    ]

    df["rates_zt_confidence"] = 1 - df[component_cols].std(axis=1).clip(0, 1)
    df["rates_zt_state"] = df["rates_zt"].apply(
        lambda x: classify_state(float(x)) if pd.notna(x) else "Insufficient Data"
    )

    output_cols = [
        "date",
        "rates_zt",
        "rates_zt_state",
        "rates_zt_confidence",
        "curve_component",
        "policy_component",
        "real_component",
        "risk_component",
        "global_component",
        "spread_10_2",
        "spread_30_5",
        "policy_gap_2y_effr",
        "real_y10",
        "term_premium_10y",
        "us_eu_10y_spread",
        "us_jp_10y_spread",
        "us_uk_10y_spread",
    ]

    out = df[output_cols].dropna(subset=["rates_zt"]).reset_index(drop=True)

    panel_out = OUT_DIR / "rates_zt_panel.parquet"
    json_out = SERVE_DIR / "rates_zt_panel.json"
    latest_out = SERVE_DIR / "rates_zt_latest.json"

    out.to_parquet(panel_out, index=False)

    out.tail(750).assign(date=lambda x: x["date"].dt.strftime("%Y-%m-%d")).to_json(
        json_out,
        orient="records",
        indent=2,
    )

    latest = out.tail(1).copy()
    latest["date"] = latest["date"].dt.strftime("%Y-%m-%d")
    latest.iloc[0].to_json(latest_out, indent=2)

    print("PASS")
    print(f"Panel: {panel_out}")
    print(f"Serving JSON: {json_out}")
    print(f"Latest JSON: {latest_out}")
    print(latest.iloc[0].to_dict())


if __name__ == "__main__":
    main()

    
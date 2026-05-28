from pathlib import Path
import numpy as np
import pandas as pd


REQUIRED_OPTIONAL_COLS = [
    "pct_above_50dma",
    "pct_above_200dma",
    "advance_decline_z",
    "new_highs_lows_z",
    "equal_weight_vs_cap_weight_z",
    "growth_value_z",
    "cyclical_defensive_z",
    "momentum_factor_z",
    "quality_factor_z",
]


def zscore(s):
    s = pd.to_numeric(s, errors="coerce")
    std = s.std()
    if std == 0 or np.isnan(std):
        return pd.Series([0.0] * len(s), index=s.index)
    return (s - s.mean()) / std


def main():
    repo_root = Path.cwd()

    in_path = repo_root / "data" / "processed" / "equities" / "market_breadth_factor_inputs.parquet"
    out_path = repo_root / "data" / "serving" / "equities" / "breadth_factor_serving_v1.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not in_path.exists():
        raise FileNotFoundError(
            f"Missing breadth/factor input file: {in_path}. "
            "Create this after sourcing breadth/factor data."
        )

    df = pd.read_parquet(in_path).copy()
    df["date"] = pd.to_datetime(df["date"])

    active_cols = []

    for col in REQUIRED_OPTIONAL_COLS:
        if col in df.columns:
            active_cols.append(col)

    if not active_cols:
        raise ValueError("No recognized breadth/factor columns found.")

    z_cols = []
    for col in active_cols:
        if col.endswith("_z"):
            z_cols.append(col)
        else:
            z_col = f"{col}_z"
            df[z_col] = zscore(df[col])
            z_cols.append(z_col)

    df["breadth_factor_score"] = df[z_cols].mean(axis=1)

    df["breadth_factor_state"] = np.where(
        df["breadth_factor_score"] >= 0.50,
        "Healthy Breadth / Risk-On Participation",
        np.where(
            df["breadth_factor_score"] <= -0.50,
            "Weak Breadth / Narrow or Stress Regime",
            "Mixed Breadth / Monitoring Regime",
        ),
    )

    latest = df.sort_values("date").iloc[-1]

    out = pd.DataFrame([{
        "date": latest["date"],
        "breadth_factor_score": latest["breadth_factor_score"],
        "breadth_factor_state": latest["breadth_factor_state"],
        "active_inputs": ", ".join(active_cols),
    }])

    out.to_parquet(out_path, index=False)

    print("OK | market breadth factor engine v1")
    print(f"output: {out_path}")
    print(out.to_string(index=False))


if __name__ == "__main__":
    main()

    
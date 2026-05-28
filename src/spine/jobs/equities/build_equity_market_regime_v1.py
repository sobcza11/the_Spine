from pathlib import Path
import numpy as np
import pandas as pd


def zscore(s):
    s = pd.to_numeric(s, errors="coerce")
    std = s.std()
    if std == 0 or np.isnan(std):
        return pd.Series([0.0] * len(s), index=s.index)
    return (s - s.mean()) / std


def main():
    repo_root = Path.cwd()

    in_path = repo_root / "data" / "serving" / "equities" / "equities_serving_v2.parquet"
    out_path = repo_root / "data" / "serving" / "equities" / "equity_market_regime_v1.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not in_path.exists():
        raise FileNotFoundError(f"Missing equity serving file: {in_path}")

    df = pd.read_parquet(in_path).copy()
    df["date"] = pd.to_datetime(df["date"])

    numeric_cols = [c for c in df.columns if c != "date" and pd.api.types.is_numeric_dtype(df[c])]

    if not numeric_cols:
        raise ValueError("No numeric equity columns found.")

    for col in numeric_cols:
        df[f"{col}_z"] = zscore(df[col])

    z_cols = [c for c in df.columns if c.endswith("_z")]

    df["equity_market_score"] = df[z_cols].mean(axis=1)

    df["equity_market_state"] = np.where(
        df["equity_market_score"] >= 0.50,
        "Risk-On / Broad Market Strength",
        np.where(
            df["equity_market_score"] <= -0.50,
            "Risk-Off / Market Stress",
            "Balanced / Monitoring Market Regime",
        ),
    )

    latest = df.sort_values("date").iloc[-1]

    out = pd.DataFrame([{
        "date": latest["date"],
        "equity_market_score": latest["equity_market_score"],
        "equity_market_state": latest["equity_market_state"],
        "input_columns": ", ".join(numeric_cols),
    }])

    out.to_parquet(out_path, index=False)

    print("OK | equity market regime v1")
    print(f"output: {out_path}")
    print(out.to_string(index=False))


if __name__ == "__main__":
    main()

    
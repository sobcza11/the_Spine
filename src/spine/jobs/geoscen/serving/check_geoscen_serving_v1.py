from pathlib import Path

import pandas as pd


def main():
    repo_root = Path.cwd()
    in_path = repo_root / "data" / "serving" / "geoscen" / "geoscen_serving_v1.parquet"

    if not in_path.exists():
        raise FileNotFoundError(f"Missing serving file: {in_path}")

    df = pd.read_parquet(in_path).copy()

    required_cols = {
        "date",
        "rbl_report_with_regime",
        "regime_label",
        "regime_confidence",
        "dominance_mean",
        "signal_strength",
        "tone_direction",
    }

    missing = required_cols - set(df.columns)
    if missing:
        raise KeyError(f"Missing required columns: {missing}")

    df["date"] = pd.to_datetime(df["date"])

    if df.empty:
        raise ValueError("Serving file has zero rows.")

    if df["date"].isna().any():
        raise ValueError("Serving file has missing dates.")

    if df["date"].duplicated().any():
        dupes = df.loc[df["date"].duplicated(), "date"].astype(str).tolist()
        raise ValueError(f"Duplicate dates found: {dupes}")

    null_checks = [
        "rbl_report_with_regime",
        "regime_label",
        "regime_confidence",
    ]

    for col in null_checks:
        if df[col].isna().any():
            raise ValueError(f"Column has null values: {col}")

    latest = df.sort_values("date").iloc[-1]

    print("OK | GeoScen serving v1 check passed")
    print(f"file: {in_path}")
    print(f"rows: {len(df)}")
    print(f"latest_date: {latest['date'].strftime('%Y-%m-%d')}")
    print(f"latest_regime: {latest['regime_label']}")
    print(f"latest_confidence: {float(latest['regime_confidence']):.3f}")
    print(f"dominance_mean: {float(latest['dominance_mean']):.3f}")
    print(f"tone_direction: {float(latest['tone_direction']):.3f}")


if __name__ == "__main__":
    main()
    
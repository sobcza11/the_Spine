from pathlib import Path
import pandas as pd


def percentile_rank(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    return numeric.rank(pct=True)


def build_cot_crowding_percentiles_v1():
    repo_root = Path.cwd()

    input_path = repo_root / "data" / "cot" / "signals" / "cot_normalized_positioning_v1.parquet"
    out_dir = repo_root / "data" / "cot" / "signals"
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(input_path).copy()

    required_cols = {
        "date",
        "instrument",
        "market_name",
        "source_name",
        "net_position_oi_ratio",
        "net_position_zscore",
        "net_position_normalized",
    }

    missing = required_cols - set(df.columns)
    if missing:
        raise KeyError(f"Missing required columns: {missing}")

    df = df.sort_values(["instrument", "date"]).reset_index(drop=True)

    df["net_position_oi_ratio"] = pd.to_numeric(
        df["net_position_oi_ratio"],
        errors="coerce",
    )

    df["net_position_zscore"] = pd.to_numeric(
        df["net_position_zscore"],
        errors="coerce",
    ).fillna(0.0)

    df["crowding_percentile"] = (
        df.groupby("instrument")["net_position_oi_ratio"]
        .transform(percentile_rank)
        .fillna(0.5)
    )

    crowding_extreme = (
        (df["crowding_percentile"] >= 0.90)
        | (df["crowding_percentile"] <= 0.10)
    )

    df["crowding_extreme_flag"] = crowding_extreme.astype("int64")

    df["crowding_direction"] = "neutral"
    df.loc[df["crowding_percentile"] >= 0.90, "crowding_direction"] = "crowded_long"
    df.loc[df["crowding_percentile"] <= 0.10, "crowding_direction"] = "crowded_short"

    df["absolute_zscore"] = df["net_position_zscore"].abs()

    df["crowding_stress_score"] = (
        0.60 * (df["crowding_percentile"] - 0.50).abs() * 2
        + 0.40 * (df["absolute_zscore"] / 3).clip(0, 1)
    ).fillna(0.0).round(4)

    signal_cols = [
        "date",
        "instrument",
        "market_name",
        "source_name",
        "net_position_oi_ratio",
        "net_position_normalized",
        "net_position_zscore",
        "crowding_percentile",
        "crowding_extreme_flag",
        "crowding_direction",
        "absolute_zscore",
        "crowding_stress_score",
    ]

    df_signal = df[signal_cols].copy()

    parquet_path = out_dir / "cot_crowding_percentiles_v1.parquet"
    json_path = out_dir / "cot_crowding_percentiles_v1.json"

    df_signal.to_parquet(parquet_path, index=False)

    df_signal.head(1000).to_json(
        json_path,
        orient="records",
        indent=2,
        date_format="iso",
    )

    latest = (
        df_signal.sort_values(["instrument", "date"])
        .groupby("instrument")
        .tail(1)
        .sort_values("crowding_stress_score", ascending=False)
    )

    latest_path = out_dir / "cot_crowding_latest_v1.parquet"
    latest_json_path = out_dir / "cot_crowding_latest_v1.json"

    latest.to_parquet(latest_path, index=False)
    latest.to_json(
        latest_json_path,
        orient="records",
        indent=2,
        date_format="iso",
    )

    print("COT crowding percentile engine complete")
    print("Rows:", len(df_signal))
    print("Instruments:", sorted(df_signal["instrument"].dropna().unique().tolist()))
    print("PARQUET:", parquet_path)
    print("JSON SAMPLE:", json_path)
    print("LATEST PARQUET:", latest_path)
    print("LATEST JSON:", latest_json_path)

    return df_signal


if __name__ == "__main__":
    build_cot_crowding_percentiles_v1()

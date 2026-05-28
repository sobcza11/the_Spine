from pathlib import Path
import pandas as pd


def build_cot_unwind_probability_v1():
    repo_root = Path.cwd()

    input_path = repo_root / "data" / "cot" / "signals" / "cot_positioning_acceleration_v1.parquet"
    out_dir = repo_root / "data" / "cot" / "signals"
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(input_path).copy()

    required_cols = {
        "date",
        "instrument",
        "market_name",
        "source_name",
        "crowding_percentile",
        "crowding_stress_score",
        "acceleration_stress_score",
        "position_acceleration_zscore",
    }

    missing = required_cols - set(df.columns)
    if missing:
        raise KeyError(f"Missing required columns: {missing}")

    df["crowding_percentile"] = pd.to_numeric(df["crowding_percentile"], errors="coerce").fillna(0.5)
    df["crowding_stress_score"] = pd.to_numeric(df["crowding_stress_score"], errors="coerce").fillna(0.0)
    df["acceleration_stress_score"] = pd.to_numeric(df["acceleration_stress_score"], errors="coerce").fillna(0.0)
    df["position_acceleration_zscore"] = pd.to_numeric(df["position_acceleration_zscore"], errors="coerce").fillna(0.0)

    df["crowding_extreme_pressure"] = ((df["crowding_percentile"] - 0.50).abs() * 2).clip(0, 1)
    df["acceleration_pressure"] = df["acceleration_stress_score"].clip(0, 1)
    df["zscore_pressure"] = (df["position_acceleration_zscore"].abs() / 3).clip(0, 1)

    df["directional_reversal_pressure"] = 0.0

    df.loc[
        (df["crowding_percentile"] >= 0.90)
        & (df["position_acceleration_zscore"] < 0),
        "directional_reversal_pressure",
    ] = 1.0

    df.loc[
        (df["crowding_percentile"] <= 0.10)
        & (df["position_acceleration_zscore"] > 0),
        "directional_reversal_pressure",
    ] = 1.0

    df["unwind_probability"] = (
        0.35 * df["crowding_extreme_pressure"]
        + 0.30 * df["acceleration_pressure"]
        + 0.20 * df["zscore_pressure"]
        + 0.15 * df["directional_reversal_pressure"]
    ).clip(0, 1).round(4)

    df["unwind_risk_state"] = "low"
    df.loc[df["unwind_probability"] >= 0.40, "unwind_risk_state"] = "watch"
    df.loc[df["unwind_probability"] >= 0.60, "unwind_risk_state"] = "elevated"
    df.loc[df["unwind_probability"] >= 0.75, "unwind_risk_state"] = "high"

    df["cot_instability_score"] = (
        0.40 * df["crowding_stress_score"]
        + 0.35 * df["acceleration_stress_score"]
        + 0.25 * df["unwind_probability"]
    ).clip(0, 1).round(4)

    signal_cols = [
        "date",
        "instrument",
        "market_name",
        "source_name",
        "crowding_percentile",
        "crowding_stress_score",
        "acceleration_stress_score",
        "position_acceleration_zscore",
        "crowding_extreme_pressure",
        "acceleration_pressure",
        "zscore_pressure",
        "directional_reversal_pressure",
        "unwind_probability",
        "unwind_risk_state",
        "cot_instability_score",
    ]

    df_signal = df[signal_cols].copy()

    parquet_path = out_dir / "cot_unwind_probability_v1.parquet"
    json_path = out_dir / "cot_unwind_probability_v1.json"

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
        .sort_values("cot_instability_score", ascending=False)
    )

    latest_path = out_dir / "cot_unwind_probability_latest_v1.parquet"
    latest_json_path = out_dir / "cot_unwind_probability_latest_v1.json"

    latest.to_parquet(latest_path, index=False)
    latest.to_json(
        latest_json_path,
        orient="records",
        indent=2,
        date_format="iso",
    )

    print("COT unwind probability scoring complete")
    print("Rows:", len(df_signal))
    print("Instruments:", sorted(df_signal["instrument"].dropna().unique().tolist()))
    print("PARQUET:", parquet_path)
    print("JSON SAMPLE:", json_path)
    print("LATEST PARQUET:", latest_path)
    print("LATEST JSON:", latest_json_path)

    return df_signal


if __name__ == "__main__":
    build_cot_unwind_probability_v1()

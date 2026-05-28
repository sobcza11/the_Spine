from pathlib import Path
import pandas as pd


def safe_zscore(series: pd.Series) -> pd.Series:

    numeric = pd.to_numeric(
        series,
        errors="coerce",
    )

    std = numeric.std()

    if pd.isna(std) or std == 0:
        return pd.Series(
            [0.0] * len(series),
            index=series.index,
        )

    return ((numeric - numeric.mean()) / std).fillna(0.0)


def build_cot_positioning_acceleration_v1():

    repo_root = Path.cwd()

    input_path = (
        repo_root
        / "data"
        / "cot"
        / "signals"
        / "cot_crowding_percentiles_v1.parquet"
    )

    out_dir = (
        repo_root
        / "data"
        / "cot"
        / "signals"
    )

    out_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    df = pd.read_parquet(input_path).copy()

    required_cols = {
        "date",
        "instrument",
        "market_name",
        "source_name",
        "net_position_normalized",
        "crowding_percentile",
        "crowding_stress_score",
    }

    missing = required_cols - set(df.columns)

    if missing:
        raise KeyError(f"Missing required columns: {missing}")

    df = (
        df
        .sort_values(["instrument", "date"])
        .reset_index(drop=True)
    )

    df["net_position_normalized"] = pd.to_numeric(
        df["net_position_normalized"],
        errors="coerce",
    ).fillna(0.0)

    df["weekly_position_change"] = (
        df.groupby("instrument")["net_position_normalized"]
        .diff()
        .fillna(0.0)
    )

    df["weekly_position_acceleration"] = (
        df.groupby("instrument")["weekly_position_change"]
        .diff()
        .fillna(0.0)
    )

    df["position_change_zscore"] = (
        df.groupby("instrument")["weekly_position_change"]
        .transform(safe_zscore)
    )

    df["position_acceleration_zscore"] = (
        df.groupby("instrument")["weekly_position_acceleration"]
        .transform(safe_zscore)
    )

    df["absolute_acceleration"] = (
        df["position_acceleration_zscore"]
        .abs()
        .fillna(0.0)
    )

    df["acceleration_stress_score"] = (
        (
            0.50
            * (
                df["absolute_acceleration"] / 3
            ).clip(0, 1)
        )
        +
        (
            0.50
            * df["crowding_stress_score"]
        )
    ).fillna(0.0).round(4)

    df["acceleration_regime"] = "stable"

    df.loc[
        df["acceleration_stress_score"] >= 0.40,
        "acceleration_regime"
    ] = "building"

    df.loc[
        df["acceleration_stress_score"] >= 0.60,
        "acceleration_regime"
    ] = "elevated"

    df.loc[
        df["acceleration_stress_score"] >= 0.80,
        "acceleration_regime"
    ] = "extreme"

    signal_cols = [
        "date",
        "instrument",
        "market_name",
        "source_name",
        "crowding_percentile",
        "crowding_stress_score",
        "weekly_position_change",
        "weekly_position_acceleration",
        "position_change_zscore",
        "position_acceleration_zscore",
        "absolute_acceleration",
        "acceleration_stress_score",
        "acceleration_regime",
    ]

    df_signal = df[signal_cols].copy()

    parquet_path = (
        out_dir
        / "cot_positioning_acceleration_v1.parquet"
    )

    json_path = (
        out_dir
        / "cot_positioning_acceleration_v1.json"
    )

    df_signal.to_parquet(
        parquet_path,
        index=False,
    )

    df_signal.head(1000).to_json(
        json_path,
        orient="records",
        indent=2,
        date_format="iso",
    )

    latest = (
        df_signal
        .sort_values(["instrument", "date"])
        .groupby("instrument")
        .tail(1)
        .sort_values(
            "acceleration_stress_score",
            ascending=False,
        )
    )

    latest_path = (
        out_dir
        / "cot_positioning_acceleration_latest_v1.parquet"
    )

    latest_json_path = (
        out_dir
        / "cot_positioning_acceleration_latest_v1.json"
    )

    latest.to_parquet(
        latest_path,
        index=False,
    )

    latest.to_json(
        latest_json_path,
        orient="records",
        indent=2,
        date_format="iso",
    )

    print("COT positioning acceleration engine complete")
    print("Rows:", len(df_signal))

    print(
        "Instruments:",
        sorted(
            df_signal["instrument"]
            .dropna()
            .unique()
            .tolist()
        )
    )

    print("PARQUET:", parquet_path)
    print("JSON SAMPLE:", json_path)
    print("LATEST PARQUET:", latest_path)
    print("LATEST JSON:", latest_json_path)

    return df_signal


if __name__ == "__main__":
    build_cot_positioning_acceleration_v1()

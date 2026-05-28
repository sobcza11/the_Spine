from pathlib import Path
import pandas as pd


def build_cot_normalized_positioning_v1():
    repo_root = Path.cwd()

    input_path = (
        repo_root
        / "data"
        / "cot"
        / "conditioning"
        / "cot_conditioned_panel_v1.parquet"
    )

    out_dir = (
        repo_root
        / "data"
        / "cot"
        / "signals"
    )

    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(input_path).copy()

    required_cols = {
        "date",
        "instrument",
        "market_name",
        "source_name",
        "long_position_raw",
        "short_position_raw",
        "open_interest_raw",
        "net_position_raw",
        "net_position_oi_ratio_raw",
        "net_position_oi_ratio_winsorized",
        "net_position_oi_ratio_scaled",
        "net_position_conditioned_zscore",
        "conditioned_percentile",
        "sample_size",
        "signal_quality_flag",
        "signal_quality_weight",
        "winsorized",
        "winsor_lower",
        "winsor_upper",
        "scaling_method",
        "rolling_window",
    }

    missing = required_cols - set(df.columns)

    if missing:
        raise KeyError(f"Missing required columns: {missing}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    df_signal = df[
        [
            "date",
            "instrument",
            "market_name",
            "source_name",
            "long_position_raw",
            "short_position_raw",
            "open_interest_raw",
            "net_position_raw",
            "net_position_oi_ratio_raw",
            "net_position_oi_ratio_winsorized",
            "net_position_oi_ratio_scaled",
            "net_position_conditioned_zscore",
            "conditioned_percentile",
            "sample_size",
            "signal_quality_flag",
            "signal_quality_weight",
            "winsorized",
            "winsor_lower",
            "winsor_upper",
            "scaling_method",
            "rolling_window",
        ]
    ].copy()

    # Backward-compatible aliases for downstream modules
    df_signal["long_position"] = df_signal["long_position_raw"]
    df_signal["short_position"] = df_signal["short_position_raw"]
    df_signal["open_interest"] = df_signal["open_interest_raw"]
    df_signal["net_position"] = df_signal["net_position_raw"]

    df_signal["net_position_oi_ratio"] = (
        df_signal["net_position_oi_ratio_scaled"]
    )

    df_signal["net_position_normalized"] = (
        df_signal["conditioned_percentile"]
    )

    df_signal["net_position_zscore"] = (
        df_signal["net_position_conditioned_zscore"]
    )

    parquet_path = (
        out_dir
        / "cot_normalized_positioning_v1.parquet"
    )

    json_path = (
        out_dir
        / "cot_normalized_positioning_v1.json"
    )

    df_signal.to_parquet(parquet_path, index=False)

    df_signal.head(1000).to_json(
        json_path,
        orient="records",
        indent=2,
        date_format="iso",
    )

    print("COT normalized positioning complete")
    print("Input: conditioned panel")
    print("Rows:", len(df_signal))
    print("Instruments:", sorted(df_signal["instrument"].dropna().unique().tolist()))
    print("Quality Counts:", df_signal["signal_quality_flag"].value_counts().to_dict())
    print("PARQUET:", parquet_path)
    print("JSON SAMPLE:", json_path)

    return df_signal


if __name__ == "__main__":
    build_cot_normalized_positioning_v1()

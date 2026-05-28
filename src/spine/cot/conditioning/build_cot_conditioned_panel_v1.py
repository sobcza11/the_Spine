from pathlib import Path
import json
import pandas as pd


WINSOR_LOWER = 0.025
WINSOR_UPPER = 0.975
ROLLING_WINDOW = 52


def find_col(df, candidates):
    lower_map = {c.lower(): c for c in df.columns}

    for candidate in candidates:
        for lower_name, original_name in lower_map.items():
            if candidate.lower() in lower_name:
                return original_name

    return None


def winsorize_by_group(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")

    lower = numeric.quantile(WINSOR_LOWER)
    upper = numeric.quantile(WINSOR_UPPER)

    if pd.isna(lower) or pd.isna(upper) or lower == upper:
        return numeric

    return numeric.clip(lower, upper)


def rolling_vol_scale(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")

    vol = (
        numeric
        .rolling(ROLLING_WINDOW, min_periods=12)
        .std()
    )

    scaled = numeric / vol.replace(0, pd.NA)

    return scaled.replace([pd.NA, float("inf"), float("-inf")], 0).fillna(0.0)


def safe_zscore(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")

    std = numeric.std()

    if pd.isna(std) or std == 0:
        return pd.Series([0.0] * len(series), index=series.index)

    return ((numeric - numeric.mean()) / std).fillna(0.0)


def signal_quality(sample_size: int) -> str:
    if sample_size >= 156:
        return "high"

    if sample_size >= 52:
        return "medium"

    if sample_size >= 26:
        return "low"

    return "sparse"


def build_cot_conditioned_panel_v1():
    repo_root = Path.cwd()

    input_path = repo_root / "data" / "cot" / "panels" / "cot_instrument_panel_v1.parquet"
    out_dir = repo_root / "data" / "cot" / "conditioning"
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(input_path).copy()

    long_col = find_col(
        df,
        [
            "noncomm_positions_long_all",
            "noncomm_positions_long",
            "noncommercial_long",
            "asset_mgr_positions_long",
            "m_money_positions_long",
            "prod_merc_positions_long",
        ],
    )

    short_col = find_col(
        df,
        [
            "noncomm_positions_short_all",
            "noncomm_positions_short",
            "noncommercial_short",
            "asset_mgr_positions_short",
            "m_money_positions_short",
            "prod_merc_positions_short",
        ],
    )

    open_interest_col = find_col(
        df,
        [
            "open_interest_all",
            "open_interest",
            "open_interest_old",
        ],
    )

    if long_col is None:
        raise KeyError(f"Could not find long positioning column. Columns: {list(df.columns)}")

    if short_col is None:
        raise KeyError(f"Could not find short positioning column. Columns: {list(df.columns)}")

    if open_interest_col is None:
        raise KeyError(f"Could not find open interest column. Columns: {list(df.columns)}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    df["long_position_raw"] = pd.to_numeric(df[long_col], errors="coerce")
    df["short_position_raw"] = pd.to_numeric(df[short_col], errors="coerce")
    df["open_interest_raw"] = pd.to_numeric(df[open_interest_col], errors="coerce")

    df["net_position_raw"] = df["long_position_raw"] - df["short_position_raw"]

    df["net_position_oi_ratio_raw"] = (
        df["net_position_raw"]
        / df["open_interest_raw"].replace(0, pd.NA)
    )

    df = (
        df
        .sort_values(["instrument", "date"])
        .reset_index(drop=True)
    )

    df["net_position_oi_ratio_winsorized"] = (
        df.groupby("instrument")["net_position_oi_ratio_raw"]
        .transform(winsorize_by_group)
    )

    df["net_position_oi_ratio_scaled"] = (
        df.groupby("instrument")["net_position_oi_ratio_winsorized"]
        .transform(rolling_vol_scale)
    )

    df["net_position_conditioned_zscore"] = (
        df.groupby("instrument")["net_position_oi_ratio_scaled"]
        .transform(safe_zscore)
    )

    df["conditioned_percentile"] = (
        df.groupby("instrument")["net_position_oi_ratio_scaled"]
        .transform(lambda x: pd.to_numeric(x, errors="coerce").rank(pct=True))
        .fillna(0.5)
    )

    sample_sizes = (
        df.groupby("instrument")
        .size()
        .rename("sample_size")
        .reset_index()
    )

    df = df.merge(sample_sizes, on="instrument", how="left")

    df["signal_quality_flag"] = df["sample_size"].apply(signal_quality)

    quality_weight_map = {
        "high": 1.00,
        "medium": 0.80,
        "low": 0.60,
        "sparse": 0.40,
    }

    df["signal_quality_weight"] = (
        df["signal_quality_flag"]
        .map(quality_weight_map)
        .fillna(0.40)
    )

    df["winsorized"] = True
    df["winsor_lower"] = WINSOR_LOWER
    df["winsor_upper"] = WINSOR_UPPER
    df["scaling_method"] = "rolling_52w_vol_scale"
    df["rolling_window"] = ROLLING_WINDOW

    output_cols = [
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

    df_out = df[output_cols].copy()

    parquet_path = out_dir / "cot_conditioned_panel_v1.parquet"
    json_path = out_dir / "cot_conditioned_panel_v1.json"
    summary_path = out_dir / "cot_conditioned_panel_summary_v1.json"

    df_out.to_parquet(parquet_path, index=False)

    df_out.head(1000).to_json(
        json_path,
        orient="records",
        indent=2,
        date_format="iso",
    )

    summary = {
        "component": "cot_conditioned_panel_v1",
        "rows": int(len(df_out)),
        "instrument_count": int(df_out["instrument"].nunique()),
        "date_min": str(df_out["date"].min()),
        "date_max": str(df_out["date"].max()),
        "winsor_lower": WINSOR_LOWER,
        "winsor_upper": WINSOR_UPPER,
        "scaling_method": "rolling_52w_vol_scale",
        "rolling_window": ROLLING_WINDOW,
        "quality_counts": df_out["signal_quality_flag"].value_counts().to_dict(),
        "instruments": sorted(df_out["instrument"].dropna().unique().tolist()),
        "status": "cot_conditioning_complete",
    }

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("COT conditioning layer complete")
    print("Rows:", len(df_out))
    print("Instruments:", sorted(df_out["instrument"].dropna().unique().tolist()))
    print("Quality Counts:", summary["quality_counts"])
    print("PARQUET:", parquet_path)
    print("JSON SAMPLE:", json_path)
    print("SUMMARY:", summary_path)
    print("Summary:", summary)

    return df_out


if __name__ == "__main__":
    build_cot_conditioned_panel_v1()

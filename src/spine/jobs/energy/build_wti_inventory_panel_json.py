from __future__ import annotations

import json
import os
from io import BytesIO
from pathlib import Path

import boto3
import pandas as pd


BUCKET = "thespine-us-hub"
WTI_INDEX_KEY = "spine_us/us_wti_index_leaf.parquet"
WTI_PANEL_KEY = "us/us_panel/df_spine_us_panel.parquet"
WTI_CANONICAL_INV_KEY = "spine_us/leaves/energy/crude_stocks_ex_spr_t2.parquet"

OUTPUT_JSON_PATH = Path("artifacts/json/wti_inventory_panel_data.json")

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("R2_ENDPOINT"),
        aws_access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY"),
        region_name=os.getenv("R2_REGION", "auto"),
    )


def load_parquet_from_r2(s3_client, key: str) -> pd.DataFrame:
    obj = s3_client.get_object(Bucket=BUCKET, Key=key)
    return pd.read_parquet(BytesIO(obj["Body"].read())).copy()


def load_wti_index_df(s3_client) -> pd.DataFrame:
    df = load_parquet_from_r2(s3_client, WTI_INDEX_KEY)

    # If the date is stored in the index, bring it out first.
    if df.index.name is not None or not isinstance(df.index, pd.RangeIndex):
        df = df.reset_index()

    date_col = None
    for candidate in ["wti_date", "as_of_date", "date", "index"]:
        if candidate in df.columns:
            date_col = candidate
            break

    if date_col is None:
        raise KeyError(
            f"WTI index parquet missing date column. Found columns: {list(df.columns)}"
        )

    req = {
        date_col,
        "wti_week_num",
        "wti_index_min",
        "wti_index_avg",
        "wti_index_max",
        "wti_index_current",
    }
    missing = req - set(df.columns)
    if missing:
        raise KeyError(f"WTI index parquet missing required columns: {sorted(missing)}")

    df = df.rename(columns={date_col: "date"}).copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    for col in [
        "wti_week_num",
        "wti_index_min",
        "wti_index_avg",
        "wti_index_max",
        "wti_index_current",
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = (
        df.loc[:, [
            "date",
            "wti_week_num",
            "wti_index_min",
            "wti_index_avg",
            "wti_index_max",
            "wti_index_current",
        ]]
        .dropna(subset=["date", "wti_week_num", "wti_index_min", "wti_index_avg", "wti_index_max"])
        .sort_values("date")
        .reset_index(drop=True)
    )
    return df

def load_wti_panel_df(s3_client) -> pd.DataFrame:
    df = load_parquet_from_r2(s3_client, WTI_PANEL_KEY)

    date_col = None
    for candidate in ["Date", "date", "as_of_date"]:
        if candidate in df.columns:
            date_col = candidate
            break

    if date_col is None:
        raise KeyError("WTI panel parquet missing date column")

    req = {
        date_col,
        "WTI_STOR_Sprd_Idx",
        "WTI_INV_Surplus",
        "WTI_INV_Std_Dev_Position",
        "WTI_STOR_Stress_Flag",
        "WTI_Seas_INV_Idx",
    }
    missing = req - set(df.columns)
    if missing:
        raise KeyError(f"WTI panel parquet missing required columns: {sorted(missing)}")

    if "leaf_name" in df.columns:
        df = df.loc[df["leaf_name"].astype(str).str.upper().eq("WTI_INV")].copy()

    df = df.rename(columns={date_col: "date"}).copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    for col in [
        "WTI_STOR_Sprd_Idx",
        "WTI_INV_Surplus",
        "WTI_INV_Std_Dev_Position",
        "WTI_Seas_INV_Idx",
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = (
        df.loc[:, [
            "date",
            "WTI_STOR_Sprd_Idx",
            "WTI_INV_Surplus",
            "WTI_INV_Std_Dev_Position",
            "WTI_STOR_Stress_Flag",
            "WTI_Seas_INV_Idx",
        ]]
        .dropna(subset=["date"])
        .sort_values("date")
        .reset_index(drop=True)
    )
    return df


def load_canonical_inv_df(s3_client) -> pd.DataFrame:
    df = load_parquet_from_r2(s3_client, WTI_CANONICAL_INV_KEY)

    req = {"symbol", "date", "value"}
    missing = req - set(df.columns)
    if missing:
        raise KeyError(f"Canonical inventory parquet missing required columns: {sorted(missing)}")

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    df = (
        df.loc[df["symbol"].astype(str).eq("WCESTUS1"), ["date", "value"]]
        .dropna(subset=["date", "value"])
        .sort_values("date")
        .reset_index(drop=True)
        .rename(columns={"value": "canonical_inventory"})
    )
    return df


def build_oc_overlay_rows(panel_df: pd.DataFrame, canonical_inv_df: pd.DataFrame) -> list[dict]:
    merged = canonical_inv_df.merge(
        panel_df.loc[:, [
            "date",
            "WTI_STOR_Sprd_Idx",
            "WTI_INV_Surplus",
            "WTI_INV_Std_Dev_Position",
            "WTI_STOR_Stress_Flag",
            "WTI_Seas_INV_Idx",
        ]],
        on="date",
        how="left",
    ).sort_values("date").reset_index(drop=True)

    rows = []
    for row in merged.itertuples(index=False):
        rows.append({
            "date": pd.Timestamp(row.date).strftime("%Y-%m-%d"),
            "oc_overlay_inventory": None if pd.isna(row.canonical_inventory) else int(row.canonical_inventory),
            "stor_sprd_idx": None if pd.isna(row.WTI_STOR_Sprd_Idx) else round(float(row.WTI_STOR_Sprd_Idx), 6),
            "inv_surplus": None if pd.isna(row.WTI_INV_Surplus) else round(float(row.WTI_INV_Surplus), 6),
            "inv_std_dev_position": None if pd.isna(row.WTI_INV_Std_Dev_Position) else round(float(row.WTI_INV_Std_Dev_Position), 6),
            "stress_flag": None if pd.isna(row.WTI_STOR_Stress_Flag) else str(row.WTI_STOR_Stress_Flag),
            "seas_inv_idx": None if pd.isna(row.WTI_Seas_INV_Idx) else round(float(row.WTI_Seas_INV_Idx), 6),
        })
    return rows


def classify_position_band(current_value: float, avg_value: float, min_value: float, max_value: float) -> str:
    band = max_value - min_value
    if band <= 0:
        return "flat_band"

    pct = (current_value - min_value) / band
    if pct <= 0.20:
        return "near_historical_low"
    if pct <= 0.40:
        return "below_average"
    if pct <= 0.60:
        return "near_average"
    if pct <= 0.80:
        return "above_average"
    return "near_historical_high"


def build_summary(index_df: pd.DataFrame, panel_df: pd.DataFrame, canonical_inv_df: pd.DataFrame) -> dict:
    latest_index = index_df.dropna(subset=["wti_index_current"]).iloc[-1]
    latest_panel = panel_df.iloc[-1]
    latest_canonical = canonical_inv_df.iloc[-1]

    position_vs_avg_pct = ((float(latest_index["wti_index_current"]) / float(latest_index["wti_index_avg"])) - 1.0) * 100.0

    return {
        "as_of_date": pd.Timestamp(latest_canonical["date"]).strftime("%Y-%m-%d"),
        "active_view_mode": "ytd_structure",
        "active_regime_overlay": "none",
        "ytd_structure": {
            "current_week": int(latest_index["wti_week_num"]),
            "current_value": round(float(latest_index["wti_index_current"]), 4),
            "historical_min": round(float(latest_index["wti_index_min"]), 4),
            "historical_avg": round(float(latest_index["wti_index_avg"]), 4),
            "historical_max": round(float(latest_index["wti_index_max"]), 4),
            "position_vs_avg_pct": round(position_vs_avg_pct, 4),
            "position_band": classify_position_band(
                float(latest_index["wti_index_current"]),
                float(latest_index["wti_index_avg"]),
                float(latest_index["wti_index_min"]),
                float(latest_index["wti_index_max"]),
            ),
        },
        "inventory_pressure": {
            "stor_sprd_idx": round(float(latest_panel["WTI_STOR_Sprd_Idx"]), 6) if pd.notna(latest_panel["WTI_STOR_Sprd_Idx"]) else None,
            "inv_std_dev_position": round(float(latest_panel["WTI_INV_Std_Dev_Position"]), 6) if pd.notna(latest_panel["WTI_INV_Std_Dev_Position"]) else None,
            "stress_flag": None if pd.isna(latest_panel["WTI_STOR_Stress_Flag"]) else str(latest_panel["WTI_STOR_Stress_Flag"]),
            "inv_surplus": round(float(latest_panel["WTI_INV_Surplus"]), 6) if pd.notna(latest_panel["WTI_INV_Surplus"]) else None,
            "canonical_inventory_last": int(latest_canonical["canonical_inventory"]),
        },
        "seasonal_stress": {
            "seas_inv_idx": round(float(latest_panel["WTI_Seas_INV_Idx"]), 6) if pd.notna(latest_panel["WTI_Seas_INV_Idx"]) else None,
            "stress_flag": None if pd.isna(latest_panel["WTI_STOR_Stress_Flag"]) else str(latest_panel["WTI_STOR_Stress_Flag"]),
            "canonical_inventory_last": int(latest_canonical["canonical_inventory"]),
        },
    }


def build_view_data(index_df: pd.DataFrame, panel_df: pd.DataFrame, canonical_inv_df: pd.DataFrame) -> dict:
    ytd_rows = []
    for row in index_df.itertuples(index=False):
        ytd_rows.append({
            "date": pd.Timestamp(row.date).strftime("%Y-%m-%d"),
            "week_num": int(row.wti_week_num),
            "historical_min": round(float(row.wti_index_min), 4),
            "historical_avg": round(float(row.wti_index_avg), 4),
            "historical_max": round(float(row.wti_index_max), 4),
            "current_value": None if pd.isna(row.wti_index_current) else round(float(row.wti_index_current), 4),
        })

    pressure_rows = []
    seasonal_rows = []
    for row in panel_df.itertuples(index=False):
        pressure_rows.append({
            "date": pd.Timestamp(row.date).strftime("%Y-%m-%d"),
            "stor_sprd_idx": None if pd.isna(row.WTI_STOR_Sprd_Idx) else round(float(row.WTI_STOR_Sprd_Idx), 6),
            "inv_surplus": None if pd.isna(row.WTI_INV_Surplus) else round(float(row.WTI_INV_Surplus), 6),
            "inv_std_dev_position": None if pd.isna(row.WTI_INV_Std_Dev_Position) else round(float(row.WTI_INV_Std_Dev_Position), 6),
            "stress_flag": None if pd.isna(row.WTI_STOR_Stress_Flag) else str(row.WTI_STOR_Stress_Flag),
            "seas_inv_idx": None if pd.isna(row.WTI_Seas_INV_Idx) else round(float(row.WTI_Seas_INV_Idx), 6),
        })
        seasonal_rows.append({
            "date": pd.Timestamp(row.date).strftime("%Y-%m-%d"),
            "seas_inv_idx": None if pd.isna(row.WTI_Seas_INV_Idx) else round(float(row.WTI_Seas_INV_Idx), 6),
            "stress_flag": None if pd.isna(row.WTI_STOR_Stress_Flag) else str(row.WTI_STOR_Stress_Flag),
        })

    oc_overlay_rows = build_oc_overlay_rows(panel_df=panel_df, canonical_inv_df=canonical_inv_df)

    return {
        "ytd_structure": {
            "chart_type": "line_band",
            "x_field": "week_num",
            "series": [
                {"name": "Historical Min", "key": "historical_min", "role": "band_low"},
                {"name": "Historical Avg", "key": "historical_avg", "role": "reference"},
                {"name": "Historical Max", "key": "historical_max", "role": "band_high"},
                {"name": "Current", "key": "current_value", "role": "primary"},
            ],
            "rows": ytd_rows,
        },
        "inventory_pressure": {
            "chart_type": "line_state",
            "x_field": "date",
            "series": [
                {"name": "Storage Spread Index", "key": "stor_sprd_idx", "role": "primary"},
                {"name": "Std Dev Position", "key": "inv_std_dev_position", "role": "secondary"},
            ],
            "rows": pressure_rows,
        },
        "seasonal_stress": {
            "chart_type": "line_state",
            "x_field": "date",
            "series": [
                {"name": "Seasonal Inventory Index", "key": "seas_inv_idx", "role": "primary"},
            ],
            "rows": seasonal_rows,
        },
        "oc_overlay": {
            "chart_type": "line_overlay",
            "x_field": "date",
            "series": [
                {"name": "OC Overlay Inventory", "key": "oc_overlay_inventory", "role": "overlay"}
            ],
            "rows": oc_overlay_rows,
        },
    }


def build_regime_overlay_data(panel_df: pd.DataFrame) -> dict:
    tight_rows = []
    surplus_rows = []
    stress_rows = []

    for row in panel_df.itertuples(index=False):
        date_str = pd.Timestamp(row.date).strftime("%Y-%m-%d")
        stress_flag = None if pd.isna(row.WTI_STOR_Stress_Flag) else str(row.WTI_STOR_Stress_Flag)
        std_pos = None if pd.isna(row.WTI_INV_Std_Dev_Position) else float(row.WTI_INV_Std_Dev_Position)

        if (std_pos is not None and std_pos < -1.0) or (stress_flag == "tight"):
            tight_rows.append({"date": date_str, "flag": True, "label": stress_flag or "tight"})
        if std_pos is not None and std_pos > 1.0:
            surplus_rows.append({"date": date_str, "flag": True, "label": "surplus"})
        if stress_flag is not None and stress_flag != "neutral":
            stress_rows.append({"date": date_str, "flag": True, "label": stress_flag})

    return {
        "tight_inventory": {
            "source": WTI_PANEL_KEY,
            "rule": "inv_std_dev_position < -1.0 OR stress_flag == 'tight'",
            "rows": tight_rows,
        },
        "surplus_inventory": {
            "source": WTI_PANEL_KEY,
            "rule": "inv_std_dev_position > 1.0",
            "rows": surplus_rows,
        },
        "stress_events": {
            "source": WTI_PANEL_KEY,
            "rule": "stress_flag != 'neutral'",
            "rows": stress_rows,
        },
    }


def build_payload(index_df: pd.DataFrame, panel_df: pd.DataFrame, canonical_inv_df: pd.DataFrame) -> dict:
    return {
        "meta": {
            "panel_id": "wti_inventory_index",
            "title": "Inventory Index",
            "default_view_mode": "ytd_structure",
            "default_regime_overlay": "none",
            "view_mode_options": ["ytd_structure", "inventory_pressure", "seasonal_stress"],
            "regime_overlay_options": ["none", "tight_inventory", "surplus_inventory", "stress_events"],
            "sources": {
                "ytd_structure": {"key": WTI_INDEX_KEY, "label": "WTI Index Leaf | the_Spine"},
                "inventory_pressure": {"key": WTI_PANEL_KEY, "label": "US Panel | WTI_INV | the_Spine"},
                "seasonal_stress": {"key": WTI_PANEL_KEY, "label": "US Panel | WTI_INV | the_Spine"},
                "oc_overlay": {"key": WTI_CANONICAL_INV_KEY, "label": "EIA canonical inventory | the_Spine"},
            },
            "status": "production_generated",
        },
        "summary": build_summary(index_df, panel_df, canonical_inv_df),
        "view_data": build_view_data(index_df, panel_df, canonical_inv_df),
        "regime_overlay_data": build_regime_overlay_data(panel_df),
    }



def _normalize_date_column(df: pd.DataFrame, label: str) -> pd.DataFrame:
    df = df.copy()

    if df.index.name is not None or not isinstance(df.index, pd.RangeIndex):
        df = df.reset_index()

    candidates = list(df.columns)

    # Prefer obvious names first
    for col in ["date", "Date", "as_of_date", "wti_date", "index"]:
        if col in df.columns:
            df = df.rename(columns={col: "date"})
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            return df

    # Fall back: first datetime-like column
    for col in candidates:
        parsed = pd.to_datetime(df[col], errors="coerce")
        if parsed.notna().sum() >= max(3, int(len(df) * 0.5)):
            df["date"] = parsed
            return df

    raise KeyError(f"{label} missing usable date column. Found columns: {list(df.columns)}")


def load_wti_index_df(s3_client) -> pd.DataFrame:
    df = load_parquet_from_r2(s3_client, WTI_INDEX_KEY)
    df = _normalize_date_column(df, "WTI index parquet")

    required = {
        "wti_week_num",
        "wti_index_min",
        "wti_index_avg",
        "wti_index_max",
        "wti_index_current",
    }
    missing = required - set(df.columns)
    if missing:
        raise KeyError(f"WTI index parquet missing required columns: {sorted(missing)} | Found: {list(df.columns)}")

    for col in required:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = (
        df.loc[:, ["date", "wti_week_num", "wti_index_min", "wti_index_avg", "wti_index_max", "wti_index_current"]]
        .dropna(subset=["date", "wti_week_num", "wti_index_min", "wti_index_avg", "wti_index_max"])
        .sort_values("date")
        .reset_index(drop=True)
    )
    return df


def load_wti_panel_df(s3_client) -> pd.DataFrame:
    df = load_parquet_from_r2(s3_client, WTI_PANEL_KEY)
    df = _normalize_date_column(df, "WTI panel parquet")

    if "leaf_name" in df.columns:
        df = df.loc[df["leaf_name"].astype(str).str.upper().eq("WTI_INV")].copy()

    required = {
        "WTI_STOR_Sprd_Idx",
        "WTI_INV_Surplus",
        "WTI_INV_Std_Dev_Position",
        "WTI_STOR_Stress_Flag",
        "WTI_Seas_INV_Idx",
    }
    missing = required - set(df.columns)
    if missing:
        raise KeyError(f"WTI panel parquet missing required columns: {sorted(missing)} | Found: {list(df.columns)}")

    for col in [
        "WTI_STOR_Sprd_Idx",
        "WTI_INV_Surplus",
        "WTI_INV_Std_Dev_Position",
        "WTI_Seas_INV_Idx",
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = (
        df.loc[:, [
            "date",
            "WTI_STOR_Sprd_Idx",
            "WTI_INV_Surplus",
            "WTI_INV_Std_Dev_Position",
            "WTI_STOR_Stress_Flag",
            "WTI_Seas_INV_Idx",
        ]]
        .dropna(subset=["date"])
        .sort_values("date")
        .reset_index(drop=True)
    )
    return df


def main() -> None:
    s3 = get_s3_client()

    index_df = load_wti_index_df(s3)
    panel_df = load_wti_panel_df(s3)
    canonical_inv_df = load_canonical_inv_df(s3)

    payload = build_payload(index_df=index_df, panel_df=panel_df, canonical_inv_df=canonical_inv_df)

    OUTPUT_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_JSON_PATH.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"wti index rows: {len(index_df)}")
    print(f"wti panel rows: {len(panel_df)}")
    print(f"canonical inv rows: {len(canonical_inv_df)}")
    print(f"as_of_date: {payload['summary']['as_of_date']}")
    print(f"wrote: {OUTPUT_JSON_PATH}")


if __name__ == "__main__":
    main()
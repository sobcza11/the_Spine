from __future__ import annotations

import json
import os
from io import BytesIO
from pathlib import Path

import boto3
import pandas as pd


BUCKET = "thespine-us-hub"
WTI_INV_T2_KEY = "spine_us/leaves/energy/crude_stocks_ex_spr_t2.parquet"
WTI_INV_OC_KEY = "raw/us/wti_inv_stor/df_wti_inv_stor_hist.parquet"

# output location outside code
OUTPUT_JSON_PATH = Path("artifacts/json/wti_inventory_data.json")


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("R2_ENDPOINT"),
        aws_access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY"),
        region_name=os.getenv("R2_REGION", "auto"),
    )


def load_inventory_t2_df(s3_client) -> pd.DataFrame:
    obj = s3_client.get_object(Bucket=BUCKET, Key=WTI_INV_T2_KEY)
    df = pd.read_parquet(BytesIO(obj["Body"].read())).copy()

    required_cols = {"symbol", "date", "value"}
    missing = required_cols - set(df.columns)
    if missing:
        raise KeyError(f"WTI inventory T2 parquet missing required columns: {sorted(missing)}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    df = (
        df.loc[:, ["symbol", "date", "value"]]
        .dropna(subset=["date", "value"])
        .sort_values("date")
        .reset_index(drop=True)
    )

    if df.empty:
        raise ValueError("No valid rows found in crude_stocks_ex_spr_t2.parquet")

    if df["symbol"].nunique() != 1:
        raise ValueError(f"Expected one inventory symbol, found: {sorted(df['symbol'].unique())}")

    return df


def load_inventory_oc_df(s3_client) -> pd.DataFrame:
    obj = s3_client.get_object(Bucket=BUCKET, Key=WTI_INV_OC_KEY)
    df = pd.read_parquet(BytesIO(obj["Body"].read())).copy()

    if "Date" not in df.columns or "INV" not in df.columns:
        raise KeyError("OC inventory history file missing required columns: ['Date', 'INV']")

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["INV"] = pd.to_numeric(df["INV"], errors="coerce")

    keep_cols = [
        "Date",
        "INV",
        "WTI_STOR_Sprd_Idx",
        "WTI_INV_Surplus",
        "WTI_INV_Std_Dev_Position",
        "WTI_STOR_Stress_Flag",
        "WTI_Seas_INV_Idx",
    ]
    keep_cols = [c for c in keep_cols if c in df.columns]

    df = (
        df.loc[:, keep_cols]
        .dropna(subset=["Date", "INV"])
        .sort_values("Date")
        .reset_index(drop=True)
    )

    return df


def build_inventory_frame(df_t2: pd.DataFrame, df_oc: pd.DataFrame) -> pd.DataFrame:
    df = df_t2.rename(columns={"value": "inventory", "symbol": "inventory_symbol"}).copy()

    # Spine-derived weekly diagnostics
    df["inv_13w_avg"] = df["inventory"].rolling(window=13, min_periods=4).mean()
    df["inv_52w_avg"] = df["inventory"].rolling(window=52, min_periods=12).mean()
    df["inv_13w_std"] = df["inventory"].rolling(window=13, min_periods=4).std()
    df["inv_z_13w"] = (df["inventory"] - df["inv_13w_avg"]) / df["inv_13w_std"]
    df["inv_vs_52w_pct"] = ((df["inventory"] / df["inv_52w_avg"]) - 1.0) * 100.0

    # Join optional OC/reference layer for labeled display
    df_oc_join = df_oc.rename(columns={"Date": "date", "INV": "oc_inventory"}).copy()
    df = df.merge(df_oc_join, on="date", how="left")

    return df


def to_json_payload(df: pd.DataFrame) -> dict:
    latest = df.iloc[-1]

    series = []
    for row in df.itertuples(index=False):
        series.append(
            {
                "date": pd.Timestamp(row.date).strftime("%Y-%m-%d"),
                "inventory": None if pd.isna(row.inventory) else int(round(float(row.inventory))),
                "inv_13w_avg": None if pd.isna(row.inv_13w_avg) else round(float(row.inv_13w_avg), 2),
                "inv_52w_avg": None if pd.isna(row.inv_52w_avg) else round(float(row.inv_52w_avg), 2),
                "inv_z_13w": None if pd.isna(row.inv_z_13w) else round(float(row.inv_z_13w), 4),
                "inv_vs_52w_pct": None if pd.isna(row.inv_vs_52w_pct) else round(float(row.inv_vs_52w_pct), 4),

                # optional OC-labeled overlay / citation-safe layer
                "oc_inventory": None if "oc_inventory" not in df.columns or pd.isna(row.oc_inventory) else int(round(float(row.oc_inventory))),
                "oc_wti_stor_sprd_idx": None if not hasattr(row, "WTI_STOR_Sprd_Idx") or pd.isna(row.WTI_STOR_Sprd_Idx) else round(float(row.WTI_STOR_Sprd_Idx), 4),
                "oc_wti_inv_surplus": None if not hasattr(row, "WTI_INV_Surplus") or pd.isna(row.WTI_INV_Surplus) else round(float(row.WTI_INV_Surplus), 4),
                "oc_wti_inv_std_dev_position": None if not hasattr(row, "WTI_INV_Std_Dev_Position") or pd.isna(row.WTI_INV_Std_Dev_Position) else round(float(row.WTI_INV_Std_Dev_Position), 4),
                "oc_wti_stor_stress_flag": None if not hasattr(row, "WTI_STOR_Stress_Flag") or pd.isna(row.WTI_STOR_Stress_Flag) else str(row.WTI_STOR_Stress_Flag),
                "oc_wti_seas_inv_idx": None if not hasattr(row, "WTI_Seas_INV_Idx") or pd.isna(row.WTI_Seas_INV_Idx) else round(float(row.WTI_Seas_INV_Idx), 4),
            }
        )

    return {
        "meta": {
            "inventory_symbol": str(latest["inventory_symbol"]),
            "primary_source": WTI_INV_T2_KEY,
            "primary_source_label": "EIA / Spine canonical inventory series",
            "overlay_source": WTI_INV_OC_KEY,
            "overlay_source_label": "OracleChambers / sourced reference layer",
            "frequency": "weekly",
            "fields": [
                "date",
                "inventory",
                "inv_13w_avg",
                "inv_52w_avg",
                "inv_z_13w",
                "inv_vs_52w_pct",
                "oc_inventory",
                "oc_wti_stor_sprd_idx",
                "oc_wti_inv_surplus",
                "oc_wti_inv_std_dev_position",
                "oc_wti_stor_stress_flag",
                "oc_wti_seas_inv_idx",
            ],
            "latest_date": pd.Timestamp(latest["date"]).strftime("%Y-%m-%d"),
            "latest_inventory": int(round(float(latest["inventory"]))),
            "latest_inv_z_13w": None if pd.isna(latest["inv_z_13w"]) else round(float(latest["inv_z_13w"]), 4),
            "latest_inv_vs_52w_pct": None if pd.isna(latest["inv_vs_52w_pct"]) else round(float(latest["inv_vs_52w_pct"]), 4),
        },
        "series": series,
    }


def main() -> None:
    s3 = get_s3_client()

    df_t2 = load_inventory_t2_df(s3)
    df_oc = load_inventory_oc_df(s3)
    df = build_inventory_frame(df_t2, df_oc)

    payload = to_json_payload(df)

    OUTPUT_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_JSON_PATH.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"rows: {len(df)}")
    print(f"latest date: {payload['meta']['latest_date']}")
    print(f"latest inventory: {payload['meta']['latest_inventory']}")
    print(f"wrote: {OUTPUT_JSON_PATH}")


if __name__ == "__main__":
    main()
    
from __future__ import annotations

import json
import os
from io import BytesIO
from pathlib import Path

import boto3
import pandas as pd


BUCKET = "thespine-us-hub"
WTI_PRICE_KEY = "spine_us/leaves/energy/wti_price_t1.parquet"

# output location outside code
OUTPUT_JSON_PATH = Path("artifacts/json/wti_price_data.json")


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("R2_ENDPOINT"),
        aws_access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY"),
        region_name=os.getenv("R2_REGION", "auto"),
    )


def load_wti_price_df(s3_client) -> pd.DataFrame:
    obj = s3_client.get_object(Bucket=BUCKET, Key=WTI_PRICE_KEY)
    df = pd.read_parquet(BytesIO(obj["Body"].read())).copy()

    required_cols = {"symbol", "date", "close"}
    missing = required_cols - set(df.columns)
    if missing:
        raise KeyError(f"WTI price parquet missing required columns: {sorted(missing)}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    df = (
        df.loc[df["symbol"].eq("WTI"), ["symbol", "date", "close"]]
        .dropna(subset=["date", "close"])
        .sort_values("date")
        .reset_index(drop=True)
    )

    if df.empty:
        raise ValueError("No valid WTI rows found in wti_price_t1.parquet")

    df["ema_20"] = df["close"].ewm(span=20, adjust=False).mean()
    df["sma_150"] = df["close"].rolling(window=150, min_periods=1).mean()

    return df


def to_json_payload(df: pd.DataFrame) -> dict:
    latest = df.iloc[-1]

    series = []
    for row in df.itertuples(index=False):
        series.append(
            {
                "date": pd.Timestamp(row.date).strftime("%Y-%m-%d"),
                "close": round(float(row.close), 4),
                "ema_20": round(float(row.ema_20), 4),
                "sma_150": round(float(row.sma_150), 4),
            }
        )

    return {
        "meta": {
            "symbol": "WTI",
            "source": "spine_us/leaves/energy/wti_price_t1.parquet",
            "frequency": "daily",
            "fields": ["date", "close", "ema_20", "sma_150"],
            "latest_date": pd.Timestamp(latest["date"]).strftime("%Y-%m-%d"),
            "latest_close": round(float(latest["close"]), 4),
            "latest_ema_20": round(float(latest["ema_20"]), 4),
            "latest_sma_150": round(float(latest["sma_150"]), 4),
        },
        "series": series,
    }


def main() -> None:
    s3 = get_s3_client()
    df = load_wti_price_df(s3)
    payload = to_json_payload(df)

    OUTPUT_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_JSON_PATH.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"rows: {len(df)}")
    print(f"latest date: {payload['meta']['latest_date']}")
    print(f"latest close: {payload['meta']['latest_close']}")
    print(f"wrote: {OUTPUT_JSON_PATH}")


if __name__ == "__main__":
    main()
    
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

    df["price_vs_ema_20_pct"] = ((df["close"] / df["ema_20"]) - 1.0) * 100.0
    df["price_vs_sma_150_pct"] = ((df["close"] / df["sma_150"]) - 1.0) * 100.0

    return df


def classify_trend_state(latest_row: pd.Series) -> str:
    close = float(latest_row["close"])
    ema_20 = float(latest_row["ema_20"])
    sma_150 = float(latest_row["sma_150"])

    if close >= ema_20 and ema_20 >= sma_150:
        if close >= sma_150 * 1.10:
            return "extended_above_long_term_average"
        return "bullish_above_short_and_long_term_average"

    if close >= sma_150 and close < ema_20:
        return "pullback_above_long_term_average"

    if close < sma_150 and close >= ema_20:
        return "short_term_rebound_below_long_term_average"

    if close < ema_20 and ema_20 < sma_150:
        return "bearish_below_short_and_long_term_average"

    return "mixed_transition_state"


def build_toolbar() -> dict:
    return {
        "default_range": "1Y",
        "range_options": ["3M", "6M", "1Y", "3Y", "5Y", "MAX"],
        "default_view": "full_overlay",
        "view_options": [
            "price",
            "price_vs_ema20",
            "price_vs_sma150",
            "full_overlay",
        ],
        "show_latest_marker": True,
        "show_ema_20": True,
        "show_sma_150": True,
    }


def to_json_payload(df: pd.DataFrame) -> dict:
    latest = df.iloc[-1]
    trend_state = classify_trend_state(latest)

    series = []
    for row in df.itertuples(index=False):
        series.append(
            {
                "date": pd.Timestamp(row.date).strftime("%Y-%m-%d"),
                "close": round(float(row.close), 4),
                "ema_20": round(float(row.ema_20), 4),
                "sma_150": round(float(row.sma_150), 4),
                "price_vs_ema_20_pct": round(float(row.price_vs_ema_20_pct), 4),
                "price_vs_sma_150_pct": round(float(row.price_vs_sma_150_pct), 4),
            }
        )

    return {
        "meta": {
            "symbol": "WTI",
            "title": "WTI Price",
            "source": WTI_PRICE_KEY,
            "source_label": "WTI / Spine canonical price series",
            "frequency": "daily",
            "units": "USD/bbl",
            "fields": [
                "date",
                "close",
                "ema_20",
                "sma_150",
                "price_vs_ema_20_pct",
                "price_vs_sma_150_pct",
            ],
            "latest_date": pd.Timestamp(latest["date"]).strftime("%Y-%m-%d"),
            "latest_close": round(float(latest["close"]), 4),
            "latest_ema_20": round(float(latest["ema_20"]), 4),
            "latest_sma_150": round(float(latest["sma_150"]), 4),
        },
        "toolbar": build_toolbar(),
        "summary": {
            "last_close": round(float(latest["close"]), 4),
            "ema_20": round(float(latest["ema_20"]), 4),
            "sma_150": round(float(latest["sma_150"]), 4),
            "price_vs_ema_20_pct": round(float(latest["price_vs_ema_20_pct"]), 4),
            "price_vs_sma_150_pct": round(float(latest["price_vs_sma_150_pct"]), 4),
            "trend_state": trend_state,
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
    print(f"trend state: {payload['summary']['trend_state']}")
    print(f"wrote: {OUTPUT_JSON_PATH}")


if __name__ == "__main__":
    main()

    
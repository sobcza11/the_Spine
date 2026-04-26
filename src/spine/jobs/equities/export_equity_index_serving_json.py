from __future__ import annotations

import io
import json
import os
import sys

import boto3
import pandas as pd


SOURCE_R2_KEY = "spine_us/us_equity_index_t1.parquet"
TARGET_R2_KEY = "spine_us/serving/equities/us_equity_index_data.json"

KEEP_SYMBOLS = ["SPY", "QQQ", "DIA", "ITOT", "MDY", "IWM"]
OUT_COLS = ["symbol", "date", "close"]


def _env(name: str, required: bool = True) -> str:
    value = os.getenv(name, "").strip()
    if required and not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def _bucket_name() -> str:
    return _env("R2_BUCKET_NAME", required=False) or _env("R2_BUCKET", required=True)


def _r2_endpoint() -> str:
    endpoint = _env("R2_ENDPOINT", required=False)
    if endpoint:
        return endpoint

    account_id = _env("R2_ACCOUNT_ID", required=True)
    return f"https://{account_id}.r2.cloudflarestorage.com"


def _r2_client():
    return boto3.client(
        "s3",
        endpoint_url=_r2_endpoint(),
        aws_access_key_id=_env("R2_ACCESS_KEY_ID", required=True),
        aws_secret_access_key=_env("R2_SECRET_ACCESS_KEY", required=True),
        region_name="auto",
    )


def _read_parquet_from_r2(key: str) -> pd.DataFrame:
    obj = _r2_client().get_object(Bucket=_bucket_name(), Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


def _upload_json_to_r2(rows: list[dict], key: str) -> None:
    body = json.dumps(rows, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

    _r2_client().put_object(
        Bucket=_bucket_name(),
        Key=key,
        Body=body,
        ContentType="application/json",
        CacheControl="no-store",
    )


def main() -> int:
    print("\n=== EXPORT EQUITY INDEX SERVING JSON ===")
    print(f"source={SOURCE_R2_KEY}")
    print(f"target={TARGET_R2_KEY}")

    df = _read_parquet_from_r2(SOURCE_R2_KEY)

    missing = {"symbol", "date", "close"} - set(df.columns)
    if missing:
        raise KeyError(f"Missing required columns: {missing}. Found: {list(df.columns)}")

    df = df.copy()
    df["symbol"] = df["symbol"].astype(str).str.upper()
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    df = (
        df[df["symbol"].isin(KEEP_SYMBOLS)]
        .dropna(subset=["date", "close"])
        .sort_values(["symbol", "date"])
        .drop_duplicates(["symbol", "date"], keep="last")
        .reset_index(drop=True)
    )

    rows = df[OUT_COLS].to_dict(orient="records")

    _upload_json_to_r2(rows, TARGET_R2_KEY)

    print("rows:", len(rows))
    print("symbols:", sorted(df["symbol"].unique().tolist()))
    print("max_date:", df["date"].max())
    print("export complete")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise


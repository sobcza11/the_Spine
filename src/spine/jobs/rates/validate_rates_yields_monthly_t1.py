import os
from io import BytesIO

import boto3
import pandas as pd

from spine.jobs.rates.rates_constants import (
    MONTHLY_RATES_MAX_LAG_DAYS,
    MONTHLY_RATES_SERIES,
    R2_ACCESS_KEY_ID_ENV,
    R2_BUCKET_ENV,
    R2_ENDPOINT_ENV,
    R2_REGION_ENV,
    R2_RATES_YIELDS_MONTHLY_T1_KEY,
    R2_SECRET_ACCESS_KEY_ENV,
)


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv(R2_ENDPOINT_ENV),
        aws_access_key_id=os.getenv(R2_ACCESS_KEY_ID_ENV),
        aws_secret_access_key=os.getenv(R2_SECRET_ACCESS_KEY_ENV),
        region_name=os.getenv(R2_REGION_ENV),
    )


def main() -> None:
    bucket = os.getenv(R2_BUCKET_ENV)
    if not bucket:
        raise ValueError(f"{R2_BUCKET_ENV} not set")

    obj = _s3_client().get_object(Bucket=bucket, Key=R2_RATES_YIELDS_MONTHLY_T1_KEY)
    df = pd.read_parquet(BytesIO(obj["Body"].read())).copy()

    expected_cols = {"symbol", "date", "value"}
    if set(df.columns) != expected_cols:
        raise ValueError(f"Schema mismatch. Expected {expected_cols}, found {set(df.columns)}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    if df[["symbol", "date", "value"]].isna().any().any():
        raise ValueError("Nulls found in canonical monthly rates leaf")

    dupes = int(df.duplicated(subset=["symbol", "date"]).sum())
    if dupes > 0:
        raise ValueError(f"Duplicate (symbol,date) rows found: {dupes}")

    expected_symbols = set(MONTHLY_RATES_SERIES.keys())
    found_symbols = set(df["symbol"].unique())
    missing_symbols = expected_symbols - found_symbols
    if missing_symbols:
        raise ValueError(f"Missing expected monthly symbols: {sorted(missing_symbols)}")

    last_date = df["date"].max()
    lag_days = (pd.Timestamp.utcnow().tz_localize(None) - last_date).days
    if lag_days > MONTHLY_RATES_MAX_LAG_DAYS:
        raise ValueError(
            f"monthly rates freshness failed. last_date={last_date.date()} "
            f"lag_days={lag_days} allowed={MONTHLY_RATES_MAX_LAG_DAYS}"
        )

    print("✅ validate_rates_yields_monthly_t1 PASSED")
    print(f"Rows: {len(df)} | Symbols: {df['symbol'].nunique()} | Last date: {last_date.date()}")
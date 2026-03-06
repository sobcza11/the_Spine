"""
Validator for deterministic sovereign rates spreads / slopes (T2).
"""

import os
from io import BytesIO

import boto3
import pandas as pd

from spine.jobs.rates.rates_constants import (
    DAILY_RATES_MAX_LAG_DAYS,
    MONTHLY_RATES_MAX_LAG_DAYS,
    R2_ACCESS_KEY_ID_ENV,
    R2_BUCKET_ENV,
    R2_ENDPOINT_ENV,
    R2_RATES_SPREADS_T2_KEY,
    R2_REGION_ENV,
    R2_SECRET_ACCESS_KEY_ENV,
    RATES_SPREADS_DAILY,
    RATES_SPREADS_MONTHLY,
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

    obj = _s3_client().get_object(Bucket=bucket, Key=R2_RATES_SPREADS_T2_KEY)
    df = pd.read_parquet(BytesIO(obj["Body"].read())).copy()

    expected_cols = {"symbol", "date", "value", "source_freq"}
    if set(df.columns) != expected_cols:
        raise ValueError(f"Schema mismatch. Expected {expected_cols}, found {set(df.columns)}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    if df[["symbol", "date", "value", "source_freq"]].isna().any().any():
        raise ValueError("Nulls found in canonical rates spreads leaf")

    dupes = int(df.duplicated(subset=["symbol", "date"]).sum())
    if dupes > 0:
        raise ValueError(f"Duplicate (symbol,date) rows found: {dupes}")

    expected_daily = set(RATES_SPREADS_DAILY.keys())
    expected_monthly = set(RATES_SPREADS_MONTHLY.keys())

    found_daily = set(df.loc[df["source_freq"] == "daily", "symbol"].unique())
    found_monthly = set(df.loc[df["source_freq"] == "monthly", "symbol"].unique())

    missing_daily = expected_daily - found_daily
    missing_monthly = expected_monthly - found_monthly

    if missing_daily:
        raise ValueError(f"Missing expected daily spread symbols: {sorted(missing_daily)}")

    if missing_monthly:
        raise ValueError(f"Missing expected monthly spread symbols: {sorted(missing_monthly)}")

    now = pd.Timestamp.utcnow().tz_localize(None)

    daily_df = df.loc[df["source_freq"] == "daily"].copy()
    if not daily_df.empty:
        last_daily = daily_df["date"].max()
        lag_daily = (now - last_daily).days
        if lag_daily > DAILY_RATES_MAX_LAG_DAYS:
            raise ValueError(
                f"daily spreads freshness failed. last_date={last_daily.date()} "
                f"lag_days={lag_daily} allowed={DAILY_RATES_MAX_LAG_DAYS}"
            )

    monthly_df = df.loc[df["source_freq"] == "monthly"].copy()
    if not monthly_df.empty:
        last_monthly = monthly_df["date"].max()
        lag_monthly = (now - last_monthly).days
        if lag_monthly > MONTHLY_RATES_MAX_LAG_DAYS:
            raise ValueError(
                f"monthly spreads freshness failed. last_date={last_monthly.date()} "
                f"lag_days={lag_monthly} allowed={MONTHLY_RATES_MAX_LAG_DAYS}"
            )

    print("✅ validate_rates_spreads_t2 PASSED")
    print(
        f"Rows: {len(df)} | Symbols: {df['symbol'].nunique()} | "
        f"Last daily: {daily_df['date'].max().date() if not daily_df.empty else 'NA'} | "
        f"Last monthly: {monthly_df['date'].max().date() if not monthly_df.empty else 'NA'}"
    )

    
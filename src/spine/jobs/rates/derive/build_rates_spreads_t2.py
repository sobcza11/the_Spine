"""
Deterministic spreads / slopes builder for sovereign rates (T2).
"""

import os
from io import BytesIO

import boto3
import botocore
import pandas as pd
from datetime import datetime


from spine.jobs.rates.rates_constants import (
    DAILY_RATES_MAX_LAG_DAYS,
    MONTHLY_RATES_MAX_LAG_DAYS,
    R2_ACCESS_KEY_ID_ENV,
    R2_BUCKET_ENV,
    R2_ENDPOINT_ENV,
    R2_RATES_SPREADS_T2_KEY,
    R2_RATES_YIELDS_DAILY_T1_KEY,
    R2_RATES_YIELDS_MONTHLY_T1_KEY,
    R2_REGION_ENV,
    R2_SECRET_ACCESS_KEY_ENV,
    RATES_SPREADS_DAILY,
    RATES_SPREADS_MONTHLY,
)

def allowed_lag_days_for_monthly(last_date: pd.Timestamp) -> int:
    # monthly macro/rates series should tolerate normal publication lag
    # while still failing genuinely stale inputs
    return 62


def validate_monthly_freshness(last_date) -> None:
    last_date = pd.to_datetime(last_date).normalize()
    today = pd.Timestamp.utcnow().normalize().tz_localize(None)
    lag_days = (today - last_date).days
    allowed = allowed_lag_days_for_monthly(last_date)

    if lag_days > allowed:
        raise ValueError(
            f"monthly spreads freshness failed. "
            f"last_date={last_date.date()} lag_days={lag_days} allowed={allowed}"
        )

def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv(R2_ENDPOINT_ENV),
        aws_access_key_id=os.getenv(R2_ACCESS_KEY_ID_ENV),
        aws_secret_access_key=os.getenv(R2_SECRET_ACCESS_KEY_ENV),
        region_name=os.getenv(R2_REGION_ENV),
    )


def _read_leaf(key: str) -> pd.DataFrame:
    bucket = os.getenv(R2_BUCKET_ENV)
    if not bucket:
        raise ValueError(f"{R2_BUCKET_ENV} not set")

    try:
        obj = _s3_client().get_object(Bucket=bucket, Key=key)
        df = pd.read_parquet(BytesIO(obj["Body"].read())).copy()
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
        return df
    except botocore.exceptions.ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "404"):
            return pd.DataFrame(columns=["symbol", "date", "value"])
        raise


def _write_leaf(df: pd.DataFrame) -> None:
    bucket = os.getenv(R2_BUCKET_ENV)
    if not bucket:
        raise ValueError(f"{R2_BUCKET_ENV} not set")

    buf = BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)

    _s3_client().put_object(
        Bucket=bucket,
        Key=R2_RATES_SPREADS_T2_KEY,
        Body=buf.getvalue(),
    )


def _compute_spreads(
    df: pd.DataFrame,
    spread_map: dict[str, tuple[str, str]],
    source_freq: str,
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["symbol", "date", "value", "source_freq"])

    out_parts: list[pd.DataFrame] = []

    for spread_symbol, (lhs_symbol, rhs_symbol) in spread_map.items():
        lhs = (
            df.loc[df["symbol"] == lhs_symbol, ["date", "value"]]
            .rename(columns={"value": "lhs_value"})
            .copy()
        )
        rhs = (
            df.loc[df["symbol"] == rhs_symbol, ["date", "value"]]
            .rename(columns={"value": "rhs_value"})
            .copy()
        )

        if lhs.empty or rhs.empty:
            continue

        merged = lhs.merge(rhs, on="date", how="inner")
        if merged.empty:
            continue

        spread = pd.DataFrame(
            {
                "symbol": spread_symbol,
                "date": merged["date"],
                "value": merged["lhs_value"] - merged["rhs_value"],
                "source_freq": source_freq,
            }
        )

        spread = (
            spread.dropna(subset=["symbol", "date", "value"])
            .sort_values(["symbol", "date"])
            .drop_duplicates(["symbol", "date"], keep="last")
            .reset_index(drop=True)
        )

        out_parts.append(spread)

    return (
        pd.concat(out_parts, ignore_index=True)
        if out_parts
        else pd.DataFrame(columns=["symbol", "date", "value", "source_freq"])
    )


def main() -> None:
    daily_df = _read_leaf(R2_RATES_YIELDS_DAILY_T1_KEY)
    monthly_df = _read_leaf(R2_RATES_YIELDS_MONTHLY_T1_KEY)

    daily_spreads = _compute_spreads(daily_df, RATES_SPREADS_DAILY, source_freq="daily")
    monthly_spreads = _compute_spreads(monthly_df, RATES_SPREADS_MONTHLY, source_freq="monthly")

    combined = pd.concat([daily_spreads, monthly_spreads], ignore_index=True)

    if combined.empty:
        raise ValueError("No rates spreads were generated")

    combined = (
        combined.dropna(subset=["symbol", "date", "value", "source_freq"])
        .sort_values(["symbol", "date"])
        .drop_duplicates(["symbol", "date"], keep="last")
        .reset_index(drop=True)
    )

    _write_leaf(combined)

    last_daily = (
        pd.to_datetime(daily_spreads["date"], errors="coerce").max()
        if not daily_spreads.empty
        else None
    )
    last_monthly = (
        pd.to_datetime(monthly_spreads["date"], errors="coerce").max()
        if not monthly_spreads.empty
        else None
    )

    now = pd.Timestamp.utcnow().tz_localize(None)

    if last_daily is not None:
        lag_daily = (now - last_daily).days
        if lag_daily > DAILY_RATES_MAX_LAG_DAYS:
            raise ValueError(
                f"daily spreads freshness failed. last_date={last_daily.date()} "
                f"lag_days={lag_daily} allowed={DAILY_RATES_MAX_LAG_DAYS}"
            )

    if last_monthly is not None:
        lag_monthly = (now - last_monthly).days
        if lag_monthly > MONTHLY_RATES_MAX_LAG_DAYS:
            raise ValueError(
                f"monthly spreads freshness failed. last_date={last_monthly.date()} "
                f"lag_days={lag_monthly} allowed={MONTHLY_RATES_MAX_LAG_DAYS}"
            )

    print("RATES SPREADS T2 UPDATE complete.")
    print(
        f"Rows: {len(combined)} | Symbols: {combined['symbol'].nunique()} | "
        f"Daily symbols: {daily_spreads['symbol'].nunique() if not daily_spreads.empty else 0} | "
        f"Monthly symbols: {monthly_spreads['symbol'].nunique() if not monthly_spreads.empty else 0}"
    )


if __name__ == "__main__":
    main()

    
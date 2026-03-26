import os
from io import BytesIO
from datetime import timedelta

import boto3
import botocore
import pandas as pd
import requests

import random
import time




from spine.jobs.rates.rates_constants import (
    DAILY_RATES_MAX_LAG_DAYS,
    DAILY_RATES_SERIES,
    FRED_API_KEY_ENV,
    R2_ACCESS_KEY_ID_ENV,
    R2_BUCKET_ENV,
    R2_ENDPOINT_ENV,
    R2_REGION_ENV,
    R2_RATES_YIELDS_DAILY_T1_KEY,
    R2_SECRET_ACCESS_KEY_ENV,
)


OVERLAP_DAYS = 10


def _fred_api_key() -> str:
    api_key = os.getenv(FRED_API_KEY_ENV)
    if not api_key:
        raise ValueError(f"{FRED_API_KEY_ENV} not set")
    return api_key


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv(R2_ENDPOINT_ENV),
        aws_access_key_id=os.getenv(R2_ACCESS_KEY_ID_ENV),
        aws_secret_access_key=os.getenv(R2_SECRET_ACCESS_KEY_ENV),
        region_name=os.getenv(R2_REGION_ENV),
    )


def _read_existing_leaf() -> pd.DataFrame:
    bucket = os.getenv(R2_BUCKET_ENV)
    if not bucket:
        raise ValueError(f"{R2_BUCKET_ENV} not set")

    try:
        obj = _s3_client().get_object(Bucket=bucket, Key=R2_RATES_YIELDS_DAILY_T1_KEY)
        df = pd.read_parquet(BytesIO(obj["Body"].read()))
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
        Key=R2_RATES_YIELDS_DAILY_T1_KEY,
        Body=buf.getvalue(),
    )


def _fetch_fred_series(symbol: str, series_id: str, observation_start: str) -> pd.DataFrame:
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": _fred_api_key(),
        "file_type": "json",
        "observation_start": observation_start,
    }
    headers = {
        "User-Agent": "the_Spine/1.0 (+GitHub Actions; deterministic rates ingest)",
        "Accept": "application/json",
    }

    for sleep_s in [0, 5, 15, 30]:
        if sleep_s > 0:
            time.sleep(sleep_s + random.uniform(0, 1.5))

        r = requests.get(url, params=params, headers=headers, timeout=60)

        if r.status_code == 403:
            print(f"WARNING: FRED 403 for {symbol} ({series_id}); retrying")
            continue

        if r.status_code == 429:
            print(f"WARNING: FRED 429 for {symbol} ({series_id}); retrying")
            continue

        if not r.ok:
            raise RuntimeError(
                f"FRED request failed for {symbol} ({series_id}) "
                f"status={r.status_code} url={r.url} body={r.text}"
            )

        observations = r.json().get("observations", [])
        df = pd.DataFrame(observations)
        if df.empty:
            return pd.DataFrame(columns=["symbol", "date", "value"])

        df["symbol"] = symbol
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

        return (
            df[["symbol", "date", "value"]]
            .dropna(subset=["symbol", "date", "value"])
            .sort_values(["symbol", "date"])
            .drop_duplicates(["symbol", "date"], keep="last")
            .reset_index(drop=True)
        )

    raise RuntimeError(f"FRED access persisted after retries for {symbol} ({series_id})")

failed_symbols = []

def main() -> None:
    existing = _read_existing_leaf()
    parts = [existing] if not existing.empty else []

    for symbol, series_id in DAILY_RATES_SERIES.items():
        symbol_existing = existing.loc[existing["symbol"] == symbol].copy()
        if symbol_existing.empty:
            start_date = "1990-01-01"
        else:
            last_dt = pd.to_datetime(symbol_existing["date"], errors="coerce").max()
            start_date = (last_dt - timedelta(days=OVERLAP_DAYS)).date().isoformat()

        try:
            df = _fetch_fred_series(symbol=symbol, series_id=series_id, observation_start=start_date)
            if not df.empty:
                parts.append(df)
            time.sleep(FRED_INTER_SYMBOL_SLEEP_S)
        except RuntimeError as e:
            print(f"WARNING: skipping {symbol} due to FRED failure: {e}")
            failed_symbols.append(symbol)
            continue

    if len(failed_symbols) > FRED_MAX_FAILED_SYMBOLS:
        raise RuntimeError(f"Too many FRED fetch failures: {failed_symbols}")

    if failed_symbols:
        print(f"Completed with partial FRED failures: {failed_symbols}")

    combined = (
        pd.concat(parts, ignore_index=True)
        if parts
        else pd.DataFrame(columns=["symbol", "date", "value"])
    )

    combined = (
        combined.dropna(subset=["symbol", "date", "value"])
        .sort_values(["symbol", "date"])
        .drop_duplicates(["symbol", "date"], keep="last")
        .reset_index(drop=True)
    )

    _write_leaf(combined)

    last_date = pd.to_datetime(combined["date"], errors="coerce").max()
    lag_days = (pd.Timestamp.utcnow().tz_localize(None) - last_date).days

    if lag_days > DAILY_RATES_MAX_LAG_DAYS:
        raise ValueError(
            f"rates_yields_daily_t1 freshness failed. "
            f"last_date={last_date.date()} lag_days={lag_days} allowed={DAILY_RATES_MAX_LAG_DAYS}"
        )
    
    if last_date is not None:
        validate_monthly_freshness(last_date)

    print("RATES YIELDS DAILY T1 UPDATE complete.")
    print(f"Rows: {len(combined)} | Last date: {last_date.date()} | lag_days={lag_days}")


if __name__ == "__main__":
    main()

def allowed_lag_days_for_monthly(last_date: pd.Timestamp) -> int:
    # monthly series should tolerate normal publication lag
    return 62


def validate_monthly_freshness(last_date) -> None:
    last_date = pd.to_datetime(last_date).normalize()
    today = pd.Timestamp.utcnow().normalize().tz_localize(None)
    lag_days = (today - last_date).days
    allowed = allowed_lag_days_for_monthly(last_date)

    if lag_days > allowed:
        raise ValueError(
            f"rates_yields_monthly_t1 freshness failed. "
            f"last_date={last_date.date()} lag_days={lag_days} allowed={allowed}"
        )

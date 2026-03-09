import os
from io import BytesIO

import boto3
import pandas as pd
import requests

from spine.jobs.rates.rates_constants import (
    DAILY_RATES_SERIES,
    FRED_API_KEY_ENV,
    R2_ACCESS_KEY_ID_ENV,
    R2_BUCKET_ENV,
    R2_ENDPOINT_ENV,
    R2_REGION_ENV,
    R2_RATES_YIELDS_DAILY_T1_KEY,
    R2_SECRET_ACCESS_KEY_ENV,
)


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


def _fetch_fred_series(symbol: str, series_id: str) -> pd.DataFrame:
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": _fred_api_key(),
        "file_type": "json",
        "observation_start": "1990-01-01",
    }

    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()

    observations = r.json().get("observations", [])
    df = pd.DataFrame(observations)
    if df.empty:
        return pd.DataFrame(columns=["symbol", "date", "value"])

    df["symbol"] = symbol
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    df = (
        df[["symbol", "date", "value"]]
        .dropna(subset=["symbol", "date", "value"])
        .sort_values(["symbol", "date"])
        .drop_duplicates(["symbol", "date"], keep="last")
        .reset_index(drop=True)
    )
    return df


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


def main() -> None:
    parts: list[pd.DataFrame] = []

    for symbol, series_id in DAILY_RATES_SERIES.items():
        df = _fetch_fred_series(symbol=symbol, series_id=series_id)
        if not df.empty:
            parts.append(df)

    combined = (
        pd.concat(parts, ignore_index=True)
        if parts
        else pd.DataFrame(columns=["symbol", "date", "value"])
    )

    combined = (
        combined.sort_values(["symbol", "date"])
        .drop_duplicates(["symbol", "date"], keep="last")
        .reset_index(drop=True)
    )

    _write_leaf(combined)
    print("RATES YIELDS DAILY T1 HIST complete.")
    print(f"Rows: {len(combined)} | Symbols: {combined['symbol'].nunique() if not combined.empty else 0}")


if __name__ == "__main__":
    main()

    
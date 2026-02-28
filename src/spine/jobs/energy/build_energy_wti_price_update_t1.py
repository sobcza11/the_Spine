import os
import requests
import pandas as pd
import boto3
from io import BytesIO
from datetime import datetime, timedelta

from spine.jobs.energy.energy_constants import (
    EIA_WTI_SERIES_ID,
    WTI_SYMBOL,
    R2_WTI_PRICE_T1_KEY,
    EIA_API_KEY_ENV,
    R2_ACCESS_KEY_ID_ENV,
    R2_SECRET_ACCESS_KEY_ENV,
    R2_BUCKET_ENV,
    R2_ENDPOINT_ENV,
    R2_REGION_ENV,
)

# ============================================================
# INCREMENTAL UPDATE — WTI PRICE T1
# ============================================================

UPDATE_BUFFER_DAYS = 45


def get_eia_api_key():
    key = os.getenv(EIA_API_KEY_ENV)
    if not key:
        raise ValueError("EIA_API_KEY not set")
    return key


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv(R2_ENDPOINT_ENV),
        aws_access_key_id=os.getenv(R2_ACCESS_KEY_ID_ENV),
        aws_secret_access_key=os.getenv(R2_SECRET_ACCESS_KEY_ENV),
        region_name=os.getenv(R2_REGION_ENV),
    )


def read_existing_leaf():
    s3 = get_s3_client()
    bucket = os.getenv(R2_BUCKET_ENV)

    try:
        obj = s3.get_object(Bucket=bucket, Key=R2_WTI_PRICE_T1_KEY)
        df = pd.read_parquet(BytesIO(obj["Body"].read()))
        return df
    except s3.exceptions.NoSuchKey:
        return pd.DataFrame()


def fetch_incremental(start_date: datetime):
    api_key = get_eia_api_key()

    url = (
        f"https://api.eia.gov/v2/seriesid/{EIA_WTI_SERIES_ID}"
        f"?api_key={api_key}"
    )

    r = requests.get(url, timeout=30)
    r.raise_for_status()

    data = r.json()

    records = data["response"]["data"]
    df = pd.DataFrame(records)

    df = df.rename(columns={
        "period": "date",
        "value": "close"
    })

    df["date"] = pd.to_datetime(df["date"])
    df = df[df["date"] >= start_date]

    df["symbol"] = WTI_SYMBOL

    df = df[["symbol", "date", "close"]]
    df = df.sort_values("date")

    return df


def upload_to_r2(df: pd.DataFrame):
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    s3 = get_s3_client()
    bucket = os.getenv(R2_BUCKET_ENV)

    s3.put_object(
        Bucket=bucket,
        Key=R2_WTI_PRICE_T1_KEY,
        Body=buffer.getvalue(),
    )


def main():
    existing = read_existing_leaf()

    if existing.empty:
        raise RuntimeError(
            "No existing leaf found. Run HIST build first."
        )

    last_date = existing["date"].max()
    start_date = last_date - timedelta(days=UPDATE_BUFFER_DAYS)

    incremental = fetch_incremental(start_date)

    combined = pd.concat([existing, incremental], ignore_index=True)
    combined = combined.drop_duplicates(subset=["symbol", "date"])
    combined = combined.sort_values("date")

    upload_to_r2(combined)

    print("WTI T1 UPDATE complete.")
    print(f"Total rows: {len(combined)}")


if __name__ == "__main__":
    main()

    
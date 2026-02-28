import os
import requests
import pandas as pd
import boto3
from io import BytesIO
from datetime import datetime

from spine.jobs.energy.energy_constants import (
    EIA_WTI_SERIES_ID,
    WTI_SYMBOL,
    R2_WTI_PRICE_T1_KEY,
    EIA_API_KEY_ENV,
    R2_ACCOUNT_ID_ENV,
    R2_ACCESS_KEY_ID_ENV,
    R2_SECRET_ACCESS_KEY_ENV,
    R2_BUCKET_ENV,
    R2_ENDPOINT_ENV,
    R2_REGION_ENV,
)

# ============================================================
# HISTORICAL BUILD — WTI PRICE T1
# ============================================================

def get_eia_api_key():
    key = os.getenv(EIA_API_KEY_ENV)
    if not key:
        raise ValueError("EIA_API_KEY not set")
    return key


def fetch_full_history():
    api_key = get_eia_api_key()

    url = (
        f"https://api.eia.gov/v2/seriesid/{EIA_WTI_SERIES_ID}"
        f"?api_key={api_key}"
    )

    r = requests.get(url, timeout=30)
    r.raise_for_status()

    data = r.json()

    if "response" not in data or "data" not in data["response"]:
        raise ValueError("Unexpected EIA response structure")

    records = data["response"]["data"]

    df = pd.DataFrame(records)

    df = df.rename(columns={
        "period": "date",
        "value": "close"
    })

    df["date"] = pd.to_datetime(df["date"])
    df["symbol"] = WTI_SYMBOL

    df = df[["symbol", "date", "close"]]
    df = df.sort_values("date").drop_duplicates(subset=["symbol", "date"])

    return df


def upload_to_r2(df: pd.DataFrame):
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv(R2_ENDPOINT_ENV),
        aws_access_key_id=os.getenv(R2_ACCESS_KEY_ID_ENV),
        aws_secret_access_key=os.getenv(R2_SECRET_ACCESS_KEY_ENV),
        region_name=os.getenv(R2_REGION_ENV),
    )

    bucket = os.getenv(R2_BUCKET_ENV)

    s3.put_object(
        Bucket=bucket,
        Key=R2_WTI_PRICE_T1_KEY,
        Body=buffer.getvalue(),
    )


def main():
    df = fetch_full_history()
    upload_to_r2(df)

    print("WTI T1 HIST build complete.")
    print(f"Rows written: {len(df)}")


if __name__ == "__main__":
    main()

    
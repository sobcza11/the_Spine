import os
from io import BytesIO
from datetime import timedelta

import boto3
import botocore
import pandas as pd
import requests

from spine.jobs.energy.energy_constants import (
    EIA_API_KEY_ENV,
    R2_ACCESS_KEY_ID_ENV,
    R2_SECRET_ACCESS_KEY_ENV,
    R2_BUCKET_ENV,
    R2_ENDPOINT_ENV,
    R2_REGION_ENV,
)

# ============================================================
# INVENTORY T2 — UPDATE BUILD (EIA)
# Series: PET.WCESTUS1.W
# ============================================================

EIA_CRUDE_STOCKS_EX_SPR_SERIES_ID = "PET.WCESTUS1.W"
CRUDE_STOCKS_EX_SPR_SYMBOL = "WCESTUS1"
R2_CRUDE_STOCKS_EX_SPR_T2_KEY = "spine_us/leaves/energy/crude_stocks_ex_spr_t2.parquet"

UPDATE_BUFFER_WEEKS = 12


def _eia_key() -> str:
    key = os.getenv(EIA_API_KEY_ENV)
    if not key:
        raise ValueError("EIA_API_KEY not set")
    return key


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv(R2_ENDPOINT_ENV),
        aws_access_key_id=os.getenv(R2_ACCESS_KEY_ID_ENV),
        aws_secret_access_key=os.getenv(R2_SECRET_ACCESS_KEY_ENV),
        region_name=os.getenv(R2_REGION_ENV),
    )


def _read_existing() -> pd.DataFrame:
    s3 = _s3_client()
    bucket = os.getenv(R2_BUCKET_ENV)
    if not bucket:
        raise ValueError("R2_BUCKET not set")

    try:
        obj = s3.get_object(Bucket=bucket, Key=R2_CRUDE_STOCKS_EX_SPR_T2_KEY)
        return pd.read_parquet(BytesIO(obj["Body"].read()))
    except botocore.exceptions.ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "404"):
            return pd.DataFrame()
        raise


def _fetch_incremental(start_dt: pd.Timestamp) -> pd.DataFrame:
    url = f"https://api.eia.gov/v2/seriesid/{EIA_CRUDE_STOCKS_EX_SPR_SERIES_ID}?api_key={_eia_key()}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    payload = r.json()

    df = pd.DataFrame(payload["response"]["data"]).copy()
    df = df.rename(columns={"period": "date", "value": "value"})
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    df = df.dropna(subset=["date", "value"])
    df = df[df["date"] >= start_dt]

    df["symbol"] = CRUDE_STOCKS_EX_SPR_SYMBOL
    df = df[["symbol", "date", "value"]]
    df = df.sort_values("date").drop_duplicates(subset=["symbol", "date"])

    return df


def _upload(df: pd.DataFrame) -> None:
    bucket = os.getenv(R2_BUCKET_ENV)
    if not bucket:
        raise ValueError("R2_BUCKET not set")

    buf = BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)

    s3 = _s3_client()
    s3.put_object(Bucket=bucket, Key=R2_CRUDE_STOCKS_EX_SPR_T2_KEY, Body=buf.getvalue())


def main():
    existing = _read_existing()

    if existing.empty:
        raise RuntimeError("No existing Inventory T2 leaf found. Run HIST build first.")

    existing["date"] = pd.to_datetime(existing["date"], errors="coerce")
    last_dt = pd.to_datetime(existing["date"].max())

    start_dt = last_dt - timedelta(weeks=int(UPDATE_BUFFER_WEEKS))

    inc = _fetch_incremental(start_dt)

    combined = pd.concat([existing, inc], ignore_index=True)
    combined["date"] = pd.to_datetime(combined["date"], errors="coerce")
    combined["value"] = pd.to_numeric(combined["value"], errors="coerce")

    combined = combined.dropna(subset=["date", "value"])
    combined = combined.drop_duplicates(subset=["symbol", "date"])
    combined = combined.sort_values(["symbol", "date"]).reset_index(drop=True)

    _upload(combined)

    print("Crude Stocks ex-SPR T2 UPDATE complete.")
    print(f"Total rows: {len(combined)} | Last date: {pd.to_datetime(combined['date'].max()).date()}")


if __name__ == "__main__":
    main()
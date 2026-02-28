import os
from io import BytesIO

import boto3
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
# INVENTORY T2 — HIST BUILD (EIA)
# Weekly U.S. Ending Stocks excluding SPR of Crude Oil
# Series: PET.WCESTUS1.W
# Canonical Leaf: spine_us/leaves/energy/crude_stocks_ex_spr_t2.parquet
# ============================================================

EIA_CRUDE_STOCKS_EX_SPR_SERIES_ID = "PET.WCESTUS1.W"
CRUDE_STOCKS_EX_SPR_SYMBOL = "WCESTUS1"
R2_CRUDE_STOCKS_EX_SPR_T2_KEY = "spine_us/leaves/energy/crude_stocks_ex_spr_t2.parquet"


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


def _fetch_full_history() -> pd.DataFrame:
    url = f"https://api.eia.gov/v2/seriesid/{EIA_CRUDE_STOCKS_EX_SPR_SERIES_ID}?api_key={_eia_key()}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    payload = r.json()

    if "response" not in payload or "data" not in payload["response"]:
        raise ValueError("Unexpected EIA response structure (missing response.data)")

    df = pd.DataFrame(payload["response"]["data"]).copy()

    # Expect v2 keys: period, value
    df = df.rename(columns={"period": "date", "value": "value"})
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    df["symbol"] = CRUDE_STOCKS_EX_SPR_SYMBOL
    df = df[["symbol", "date", "value"]]
    df = df.dropna(subset=["date", "value"])
    df = df.sort_values(["symbol", "date"]).drop_duplicates(subset=["symbol", "date"]).reset_index(drop=True)

    return df


def _upload(df: pd.DataFrame) -> None:
    bucket = os.getenv(R2_BUCKET_ENV)
    if not bucket:
        raise ValueError("R2_BUCKET not set")

    buf = BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)

    s3 = _s3_client()
    s3.put_object(
        Bucket=bucket,
        Key=R2_CRUDE_STOCKS_EX_SPR_T2_KEY,
        Body=buf.getvalue(),
    )


def main():
    df = _fetch_full_history()
    _upload(df)

    last_dt = pd.to_datetime(df["date"].max()).date()
    print("Crude Stocks ex-SPR T2 HIST build complete.")
    print(f"Rows written: {len(df)} | Last date: {last_dt}")


if __name__ == "__main__":
    main()

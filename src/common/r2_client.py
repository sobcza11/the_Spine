import io
import os

from typing import Optional

import boto3
import pandas as pd
from botocore.config import Config


def get_r2_client():
    """
    Create an S3-compatible client for Cloudflare R2.

    Requires env vars:
        R2_ACCOUNT_ID
        R2_ACCESS_KEY_ID
        R2_SECRET_ACCESS_KEY
    """
    account_id = os.environ.get("R2_ACCOUNT_ID")
    access_key = os.environ.get("R2_ACCESS_KEY_ID")
    secret_key = os.environ.get("R2_SECRET_ACCESS_KEY")

    if not account_id:
        raise RuntimeError(
            "R2_ACCOUNT_ID env var is missing or empty – check GitHub Secrets / local env."
        )
    if not access_key or not secret_key:
        raise RuntimeError(
            "R2_ACCESS_KEY_ID or R2_SECRET_ACCESS_KEY is missing – "
            "check GitHub Secrets / local env."
        )

    endpoint_url = f"https://{account_id}.r2.cloudflarestorage.com"

    if endpoint_url == "https://.r2.cloudflarestorage.com":
        # Extra guardrail in case account_id is somehow an empty string
        raise RuntimeError(f"Invalid R2 endpoint_url constructed: {endpoint_url!r}")

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
    )


def _get_bucket_name() -> str:
    bucket = os.environ.get("R2_BUCKET_NAME")
    if not bucket:
        raise RuntimeError("R2_BUCKET_NAME env var is missing or empty.")
    return bucket


def read_parquet_from_r2(key: str, *, columns: Optional[list[str]] = None) -> pd.DataFrame:
    """
    Read a Parquet file from R2 into a pandas DataFrame.

    Args:
        key: Object key inside the R2 bucket, e.g. "spine_us/us_fx_spot_canonical.parquet"
        columns: Optional list of columns to read.

    Returns:
        pd.DataFrame
    """
    client = get_r2_client()
    bucket = _get_bucket_name()

    resp = client.get_object(Bucket=bucket, Key=key)
    data = resp["Body"].read()

    # Let pandas / pyarrow handle the parquet decoding
    return pd.read_parquet(io.BytesIO(data), columns=columns)


def write_parquet_to_r2(df: pd.DataFrame, key: str) -> None:
    """
    Write a pandas DataFrame to R2 as a Parquet file.

    Args:
        df: DataFrame to write.
        key: Object key inside the R2 bucket.
    """
    client = get_r2_client()
    bucket = _get_bucket_name()

    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)

    client.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())

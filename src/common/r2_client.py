from __future__ import annotations

import io
import os

import boto3
import pandas as pd
from botocore.config import Config


def get_r2_client():
    """
    Return a boto3 S3 client configured for Cloudflare R2 using
    S3-compatible API keys.

    Required env vars:
        R2_ACCOUNT_ID
        R2_ACCESS_KEY_ID
        R2_SECRET_ACCESS_KEY
    Optional:
        R2_BUCKET  (used by helpers below)
    """
    account_id = os.environ["R2_ACCOUNT_ID"]
    access_key = os.environ["R2_ACCESS_KEY_ID"]
    secret_key = os.environ["R2_SECRET_ACCESS_KEY"]

    return boto3.client(
        "s3",
        endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4", region_name="auto"),
    )


def read_parquet_from_r2(key: str) -> pd.DataFrame:
    """
    Read a Parquet file from R2 into a DataFrame.
    Uses R2_BUCKET env var for the bucket name.
    """
    client = get_r2_client()
    bucket = os.environ["R2_BUCKET"]

    obj = client.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()
    return pd.read_parquet(io.BytesIO(body))


def write_parquet_to_r2(df: pd.DataFrame, key: str, index: bool = True) -> None:
    """
    Write a DataFrame as Parquet to R2.
    Uses R2_BUCKET env var for the bucket name.
    """
    client = get_r2_client()
    bucket = os.environ["R2_BUCKET"]

    buf = io.BytesIO()
    df.to_parquet(buf, index=index)
    buf.seek(0)

    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue(),
        ContentType="application/octet-stream",
    )


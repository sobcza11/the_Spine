from __future__ import annotations

import os
import pathlib
from typing import Optional

import boto3
import pandas as pd

from Spine.us_panel import build_spine_us_panel


# -------------------------------
# Paths
# -------------------------------
THIS_FILE = pathlib.Path(__file__).resolve()
# the_Spine/ as project root
ROOT_DIR = THIS_FILE.parents[2]
EXPORTS_DIR = ROOT_DIR / "exports"
EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

LOCAL_PARQUET = EXPORTS_DIR / "df_spine_us_panel.parquet"


# -------------------------------
# R2 client factory
# -------------------------------
def _get_r2_client():
    account_id = os.environ["R2_ACCOUNT_ID"]
    access_key = os.environ["R2_ACCESS_KEY_ID"]
    secret_key = os.environ["R2_SECRET_ACCESS_KEY"]

    endpoint_url = f"https://{account_id}.r2.cloudflarestorage.com"

    session = boto3.session.Session()
    s3 = session.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    return s3


# -------------------------------
# Export function
# -------------------------------
def export_spine_us_panel_to_r2(
    as_of: Optional[pd.Timestamp] = None,
    key: str = "us/us_panel/df_spine_us_panel.parquet",
) -> str:
    """
    Build the U.S. Spine panel, save as Parquet & upload to R2.

    - Uses build_spine_us_panel() for the data
    - Writes to exports/df_spine_us_panel.parquet
    - Uploads to R2 bucket defined in env var R2_BUCKET_NAME

    Returns:
        The R2 object key used.
    """
    # 1) Build panel
    df_us = build_spine_us_panel(as_of=as_of)

    # 2) Save locally as Parquet
    LOCAL_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    df_us.to_parquet(LOCAL_PARQUET)

    # 3) Upload to R2
    bucket_name = os.environ["R2_BUCKET_NAME"]
    s3 = _get_r2_client()
    s3.upload_file(str(LOCAL_PARQUET), bucket_name, key)

    return key


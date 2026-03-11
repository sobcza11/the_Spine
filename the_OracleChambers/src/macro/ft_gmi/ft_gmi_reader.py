from __future__ import annotations

import os
from io import BytesIO

import boto3
import pandas as pd


R2_BUCKET = os.getenv("R2_BUCKET") or os.getenv("R2_BUCKET_NAME")
R2_ENDPOINT = os.getenv("R2_ENDPOINT")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_REGION = os.getenv("R2_REGION")
R2_FT_GMI_KEY = os.getenv("R2_FT_GMI_KEY", "lab/ft_gmi/ft_gmi_daily.parquet")

FT_GMI_KEY = "lab/ft_gmi/ft_gmi_daily.parquet"

def _s3():
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name="auto",
    )


def read_ft_gmi_daily() -> pd.DataFrame:
    if not R2_BUCKET:
        raise ValueError("R2 bucket not set")
    obj = _s3().get_object(Bucket=R2_BUCKET, Key=R2_FT_GMI_KEY)
    df = pd.read_parquet(BytesIO(obj["Body"].read())).copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df.sort_values("date").reset_index(drop=True)


def read_ft_gmi_latest() -> dict:
    df = read_ft_gmi_daily()
    if df.empty:
        raise ValueError("FT-GMI leaf is empty")
    return df.iloc[-1].to_dict()

def load_ft_gmi():
    obj = _s3().get_object(Bucket=R2_BUCKET, Key=FT_GMI_KEY)
    df = pd.read_parquet(BytesIO(obj["Body"].read()))
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date")


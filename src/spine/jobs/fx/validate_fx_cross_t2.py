import os
import pandas as pd
from io import BytesIO
from datetime import datetime
import boto3

R2_ENDPOINT = os.environ["R2_ENDPOINT"]
R2_ACCESS_KEY_ID = os.environ["R2_ACCESS_KEY_ID"]
R2_SECRET_ACCESS_KEY = os.environ["R2_SECRET_ACCESS_KEY"]
R2_BUCKET = os.environ["R2_BUCKET"]

TARGET_KEY = "spine_us/us_fx_spot_cross_t2.parquet"

def get_r2():
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    )

def read_parquet(client, key):
    obj = client.get_object(Bucket=R2_BUCKET, Key=key)
    return pd.read_parquet(BytesIO(obj["Body"].read()))

def main():
    r2 = get_r2()
    df = read_parquet(r2, TARGET_KEY)

    required = {"symbol", "date", "close"}
    if not required.issubset(df.columns):
        raise ValueError("Missing required columns.")

    if set(df["symbol"].unique()) != {"NZDAUD"}:
        raise ValueError("Unexpected symbol universe.")

    if df.duplicated(subset=["symbol", "date"]).any():
        raise ValueError("Duplicate rows detected.")

    if df["close"].isnull().any():
        raise ValueError("Null values in close.")

    if (df["close"] <= 0).any():
        raise ValueError("Non-positive values in close.")

    max_date = pd.to_datetime(df["date"]).max()
    if (datetime.utcnow() - max_date).days > 3:
        raise ValueError("Data freshness violation.")

    print("NZDAUD validation passed.")

if __name__ == "__main__":
    main()
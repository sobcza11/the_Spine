import os
import pandas as pd
from io import BytesIO
import boto3

R2_ENDPOINT = os.environ["R2_ENDPOINT"]
R2_ACCESS_KEY_ID = os.environ["R2_ACCESS_KEY_ID"]
R2_SECRET_ACCESS_KEY = os.environ["R2_SECRET_ACCESS_KEY"]
R2_BUCKET = os.environ["R2_BUCKET"]

SOURCE_KEY = "spine_us/us_fx_spot_canonical_t2.parquet"
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

def write_parquet(client, df, key):
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    client.put_object(Bucket=R2_BUCKET, Key=key, Body=buffer.getvalue())

def main():
    r2 = get_r2()
    df = read_parquet(r2, SOURCE_KEY)

    df = df[df["symbol"].isin(["AUDUSD", "NZDUSD"])].copy()

    aud = df[df["symbol"] == "AUDUSD"][["date", "close"]].rename(columns={"close": "aud"})
    nzd = df[df["symbol"] == "NZDUSD"][["date", "close"]].rename(columns={"close": "nzd"})

    merged = pd.merge(nzd, aud, on="date", how="inner")
    merged["close"] = merged["nzd"] / merged["aud"]
    merged["symbol"] = "NZDAUD"

    out = merged[["symbol", "date", "close"]].sort_values("date").reset_index(drop=True)

    write_parquet(r2, out, TARGET_KEY)
    print("NZDAUD HIST written.")

if __name__ == "__main__":
    main()


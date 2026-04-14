import os
import sys
import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta

REQUIRED_INPUTS = {
    "fx": "spine_us/leaves/fx/fx_cross_t2.parquet",
    "rates": "spine_us/leaves/rates/rates_yields_daily_t1.parquet",
    "energy": "spine_us/leaves/energy/wti_price_t1.parquet",
    "equity": "spine_us/leaves/equity/vinv_equal_weight_t3.parquet"
}

REQUIRED_COLUMNS = {
    "date"
}

MAX_STALENESS_DAYS = 5
MIN_ROWS = 100


def get_r2_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["R2_ENDPOINT"],
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name=os.environ.get("R2_REGION", "auto"),
    )


def load_parquet_from_r2(client, bucket, key):
    obj = client.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(BytesIO(obj["Body"].read()))


def validate_dataset(name, df):

    if len(df) < MIN_ROWS:
        raise ValueError(f"{name}: insufficient rows ({len(df)})")

    missing_cols = REQUIRED_COLUMNS - set(df.columns)
    if missing_cols:
        raise ValueError(f"{name}: missing columns {missing_cols}")

    if df["date"].isna().any():
        raise ValueError(f"{name}: null values in date column")

    if not df["date"].is_monotonic_increasing:
        raise ValueError(f"{name}: date column not monotonic")

    latest = pd.to_datetime(df["date"].max())
    if latest < datetime.utcnow() - timedelta(days=MAX_STALENESS_DAYS):
        raise ValueError(f"{name}: data stale (latest={latest})")


def main():

    bucket = os.environ["R2_BUCKET"]
    client = get_r2_client()

    for name, key in REQUIRED_INPUTS.items():

        print(f"Checking {name} input...")

        try:
            df = load_parquet_from_r2(client, bucket, key)
        except Exception as e:
            raise RuntimeError(f"Failed to load {name} dataset from {key}: {e}")

        validate_dataset(name, df)

        print(f"{name} OK ({len(df)} rows)")

    print("All FT-GMI inputs validated successfully.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("VALIDATION FAILED:", str(e))
        sys.exit(1)
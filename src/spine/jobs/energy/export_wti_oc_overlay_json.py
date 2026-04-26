import pandas as pd
import numpy as np
import boto3
import io
import os
import json

def s3():
    endpoint = os.environ.get("R2_ENDPOINT")
    if not endpoint:
        endpoint = f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
    )

def bucket():
    return os.environ["R2_BUCKET_NAME"]

def read_parquet(key):
    obj = s3().get_object(Bucket=bucket(), Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))

def main():
    KEY = "spine_us/leaves/energy/crude_stocks_ex_spr_t2.parquet"

    df = read_parquet(KEY)

    if df.empty:
        raise RuntimeError("WTI inventory parquet empty.")

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    # 15Y rolling seasonal normalization
    df["rolling_mean"] = df["value"].rolling(52 * 15, min_periods=52).mean()
    df["rolling_std"]  = df["value"].rolling(52 * 15, min_periods=52).std()

    df["z"] = (df["value"] - df["rolling_mean"]) / df["rolling_std"]

    latest = df.dropna(subset=["z"]).tail(1)

    if latest.empty:
        raise RuntimeError("No overlay data available.")

    r = latest.iloc[0]

    payload = {
        "meta": {
            "source": "EIA",
            "as_of_date": r["date"].strftime("%Y-%m-%d")
        },
        "overlay": {
            "z": float(r["z"]),
            "state": "tight" if r["z"] > 1 else "loose" if r["z"] < -1 else "neutral"
        }
    }

    OUT_KEY = "spine_us/serving/wti/wti_inventory_oc_overlay.json"

    s3().put_object(
        Bucket=bucket(),
        Key=OUT_KEY,
        Body=json.dumps(payload, indent=2).encode("utf-8"),
        ContentType="application/json"
    )

    print("WTI OC overlay export complete.")
    print(f"as_of={payload['meta']['as_of_date']}")

if __name__ == "__main__":
    main()


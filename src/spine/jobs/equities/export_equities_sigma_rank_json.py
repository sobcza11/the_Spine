import pandas as pd
import numpy as np
import boto3
import io
import os
import json

WINDOW = 20

def s3():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["R2_ENDPOINT"],
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
    )

def bucket():
    return os.environ["R2_BUCKET_NAME"]

def read_parquet(key):
    obj = s3().get_object(Bucket=bucket(), Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))

def main():
    KEY = "spine_us/us_equity_index_t1.parquet"

    df = read_parquet(KEY)

    if df.empty:
        raise RuntimeError("Equity parquet empty.")

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["symbol", "date"])

    df["ret"] = df.groupby("symbol")["close"].pct_change()
    df["vol"] = df.groupby("symbol")["ret"].rolling(WINDOW).std().reset_index(level=0, drop=True) * np.sqrt(252)

    latest = df.dropna(subset=["vol"]).groupby("symbol").tail(1)

    if latest.empty:
        raise RuntimeError("No sigma rows.")

    rows = []

    for _, r in latest.iterrows():
        rows.append({
            "symbol": r["symbol"],
            "pair": r["symbol"],
            "as_of_date": r["date"].strftime("%Y-%m-%d"),
            "realized_vol_20d": float(r["vol"])
        })

    vols = np.array([r["realized_vol_20d"] for r in rows])
    mean = vols.mean()
    std = vols.std() if vols.std() != 0 else 1

    for r in rows:
        r["z"] = float((r["realized_vol_20d"] - mean) / std)

    rows = sorted(rows, key=lambda x: x["z"], reverse=True)

    for i, r in enumerate(rows, start=1):
        r["rank"] = i
        r["state"] = "easy" if r["z"] > 0 else "tight"

    OUT_KEY = "spine_us/serving/equities/equities_sigma_rank.json"

    s3().put_object(
        Bucket=bucket(),
        Key=OUT_KEY,
        Body=json.dumps(rows, indent=2).encode("utf-8"),
        ContentType="application/json"
    )

    print("Equities sigma export complete.")
    print(f"as_of={rows[0]['as_of_date']}")

if __name__ == "__main__":
    main()
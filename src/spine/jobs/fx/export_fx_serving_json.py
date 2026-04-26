from __future__ import annotations

import io
import json
import os
from pathlib import Path

import boto3
import pandas as pd


OUT_DIR = Path("data/serving/fx")

R2_PRICE_OUT = "fx_price_data.json"
R2_SPREADS_OUT = "fx_spreads_data.json"
R2_SIGMA_OUT = "fx_sigma_data.json"

FX_PREFIXES = [
    "spine_us/leaves/fx/",
    "spine_us/leaves/fx_spot/",
    "spine_us/leaves/fx_cross/",
    "spine_us/leaves/currency/",
    "spine_us/leaves/currencies/",
]

FX_SPOT_KEY = "spine_us/us_fx_spot_canonical_t1.parquet"
FX_SPOT_T2_KEY = "spine_us/us_fx_spot_canonical_t2.parquet"
FX_SPREADS_KEY = "spine_us/us_fx_10y_spreads.parquet"

def env(name):
    value = os.getenv(name) or os.getenv(name.replace("R2_BUCKET", "R2_BUCKET_NAME"))
    if not value:
        raise RuntimeError(f"Missing env var: {name}")
    return value


def bucket():
    return os.getenv("R2_BUCKET") or env("R2_BUCKET_NAME")


def s3():
    return boto3.client(
        "s3",
        endpoint_url=env("R2_ENDPOINT"),
        aws_access_key_id=env("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=env("R2_SECRET_ACCESS_KEY"),
        region_name=os.getenv("R2_REGION", "auto"),
    )


def list_parquet_keys():
    client = s3()
    keys = []

    for prefix in FX_PREFIXES:
        token = None

        while True:
            kwargs = {"Bucket": bucket(), "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token

            resp = client.list_objects_v2(**kwargs)

            for obj in resp.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".parquet"):
                    keys.append(key)

            if not resp.get("IsTruncated"):
                break

            token = resp.get("NextContinuationToken")

    return sorted(set(keys))


def read_parquet(key):
    obj = s3().get_object(Bucket=bucket(), Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


def write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def upload_json(key, payload):
    s3().put_object(
        Bucket=bucket(),
        Key=key,
        Body=json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
        CacheControl="no-store",
    )


def normalize_date_col(df):
    for col in ["date", "as_of_date", "timestamp"]:
        if col in df.columns:
            df["date"] = pd.to_datetime(df[col], errors="coerce")
            return df
    return df


def find_price_df(frames):
    for key, df in frames:
        cols = set(df.columns)
        if {"date", "symbol"}.issubset(cols) and any(c in cols for c in ["close", "price", "last", "value"]):
            if "spread" not in key.lower() and "sigma" not in key.lower():
                return key, df
    raise RuntimeError("No FX price parquet found.")


def find_spreads_df(frames):
    for key, df in frames:
        cols = set(df.columns)
        if "spread" in key.lower() or "yld_10y_diff" in cols or "spread" in cols:
            return key, df
    raise RuntimeError("No FX spreads parquet found.")


def find_sigma_df(frames):
    for key, df in frames:
        cols = set(df.columns)
        if "sigma" in key.lower() or {"rank", "z"}.issubset(cols) or {"sigma_rank", "sigma_z"}.issubset(cols):
            return key, df
    raise RuntimeError("No FX sigma parquet found.")


def build_price_payload(df):
    df = normalize_date_col(df.copy())

    value_col = next((c for c in ["close", "price", "last", "value"] if c in df.columns), None)
    if not value_col:
        raise RuntimeError("FX price dataframe missing close/price/last/value column.")

    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
    df["close"] = pd.to_numeric(df[value_col], errors="coerce")
    df = df.dropna(subset=["date", "symbol", "close"]).sort_values(["symbol", "date"])

    payload = {}

    for symbol, group in df.groupby("symbol"):
        payload[symbol] = [
            {
                "date": row["date"].strftime("%Y-%m-%d"),
                "close": float(row["close"]),
                "open": float(row["close"]),
                "high": float(row["close"]),
                "low": float(row["close"]),
            }
            for _, row in group.iterrows()
        ]

    return payload


def build_spreads_payload(df):
    df = normalize_date_col(df.copy())

    symbol_col = next((c for c in ["symbol", "pair", "fx_pair"] if c in df.columns), None)
    value_col = next((c for c in ["yld_10y_diff", "spread", "value", "close", "price", "last"] if c in df.columns), None)

    if not symbol_col or not value_col:
        raise RuntimeError("FX spreads dataframe missing symbol/pair and spread/close column.")

    df["symbol"] = df[symbol_col].astype(str).str.upper().str.replace("/", "", regex=False).str.strip()
    df["raw_value"] = pd.to_numeric(df[value_col], errors="coerce")
    df = df.dropna(subset=["date", "symbol", "raw_value"]).sort_values(["symbol", "date"])

    payload = {}

    for symbol, group in df.groupby("symbol"):
        group = group.copy()
        group["yld_10y_diff"] = group["raw_value"] - group["raw_value"].rolling(30, min_periods=1).mean()

        payload[symbol] = [
            {
                "as_of_date": row["date"].strftime("%Y-%m-%d"),
                "base_ccy": symbol[:3],
                "quote_ccy": symbol[3:],
                "yld_10y_base": None,
                "yld_10y_quote": None,
                "yld_10y_diff": float(row["yld_10y_diff"]),
            }
            for _, row in group.iterrows()
        ]

    return payload


def build_sigma_payload(df):
    df = normalize_date_col(df.copy())

    symbol_col = next((c for c in ["symbol", "pair", "fx_pair"] if c in df.columns), None)
    z_col = next((c for c in ["z", "sigma_z", "sigma_value", "value"] if c in df.columns), None)
    rank_col = next((c for c in ["rank", "sigma_rank"] if c in df.columns), None)

    if not symbol_col:
        raise RuntimeError("FX sigma dataframe missing symbol/pair column.")

    df["symbol"] = df[symbol_col].astype(str).str.upper().str.replace("/", "", regex=False).str.strip()

    if z_col:
        df["z"] = pd.to_numeric(df[z_col], errors="coerce")
    else:
        value_col = next((c for c in ["close", "price", "last", "value"] if c in df.columns), None)
        if not value_col:
            raise RuntimeError("FX sigma fallback missing close/price/last/value column.")

        df["close"] = pd.to_numeric(df[value_col], errors="coerce")
        df = df.dropna(subset=["date", "symbol", "close"]).sort_values(["symbol", "date"])
        df["ret"] = df.groupby("symbol")["close"].pct_change()
        df["vol_20"] = df.groupby("symbol")["ret"].transform(lambda x: x.rolling(20).std())
        latest_vol = df.dropna(subset=["vol_20"]).loc[df.dropna(subset=["vol_20"]).groupby("symbol")["date"].idxmax()].copy()

        mean_vol = latest_vol["vol_20"].mean()
        std_vol = latest_vol["vol_20"].std()

        latest_vol["z"] = (latest_vol["vol_20"] - mean_vol) / std_vol if std_vol and std_vol > 0 else 0
        latest_vol["rank"] = latest_vol["z"].rank(method="first", ascending=True)

        return [
            {
                "symbol": row["symbol"],
                "z": float(row["z"]),
                "rank": int(row["rank"]),
                "pct": None,
                "state": "FallbackVol",
                "as_of_date": row["date"].strftime("%Y-%m-%d"),
            }
            for _, row in latest_vol.sort_values("rank").iterrows()
        ]

    df["rank"] = pd.to_numeric(df[rank_col], errors="coerce") if rank_col else None
    df = df.dropna(subset=["date", "symbol", "z"]).sort_values(["symbol", "date"])

    latest_rows = df.loc[df.groupby("symbol")["date"].idxmax()].copy()

    if "rank" not in latest_rows.columns or latest_rows["rank"].isna().all():
        latest_rows["rank"] = latest_rows["z"].rank(method="first", ascending=False)

    latest_rows = latest_rows.sort_values("rank")

    return [
        {
            "symbol": row["symbol"],
            "z": float(row["z"]),
            "rank": int(row["rank"]),
            "pct": None,
            "state": "Live",
            "as_of_date": row["date"].strftime("%Y-%m-%d"),
        }
        for _, row in latest_rows.iterrows()
    ]


def main():
    price_key = FX_SPOT_KEY
    spreads_key = FX_SPREADS_KEY
    sigma_key = FX_SPOT_KEY

    price_df = read_parquet(price_key)
    spreads_df = read_parquet(spreads_key)
    sigma_df = price_df
        
    price_payload = build_price_payload(price_df)
    spreads_payload = build_spreads_payload(spreads_df)
    sigma_payload = build_sigma_payload(sigma_df)

    write_json(OUT_DIR / R2_PRICE_OUT, price_payload)
    write_json(OUT_DIR / R2_SPREADS_OUT, spreads_payload)
    write_json(OUT_DIR / R2_SIGMA_OUT, sigma_payload)

    upload_json(R2_PRICE_OUT, price_payload)
    upload_json(R2_SPREADS_OUT, spreads_payload)
    upload_json(R2_SIGMA_OUT, sigma_payload)

    latest_price = max(
        row["date"]
        for rows in price_payload.values()
        for row in rows
    ) if price_payload else "--"

    latest_spread = max(
        row["as_of_date"]
        for rows in spreads_payload.values()
        for row in rows
    ) if spreads_payload else "--"

    latest_sigma = max(
        row["as_of_date"]
        for row in sigma_payload
    ) if sigma_payload else "--"

    print("FX serving export complete.")
    print(f"price_key={price_key}")
    print(f"spreads_key={spreads_key}")
    print(f"sigma_key={sigma_key}")
    print(f"price_latest={latest_price}")
    print(f"spreads_latest={latest_spread}")
    print(f"sigma_latest={latest_sigma}")


if __name__ == "__main__":
    main()


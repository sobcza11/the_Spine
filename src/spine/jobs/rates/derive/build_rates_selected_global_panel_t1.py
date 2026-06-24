import json
import os
from io import BytesIO

import boto3
import pandas as pd

from spine.jobs.rates.rates_constants import (
    R2_ACCESS_KEY_ID_ENV,
    R2_BUCKET_ENV,
    R2_ENDPOINT_ENV,
    R2_REGION_ENV,
    R2_RATES_YIELDS_MONTHLY_T1_KEY,
    R2_SECRET_ACCESS_KEY_ENV,
)

OUT_KEY = "spine_us/serving/rates/rates_selected_global_panel.json"


COUNTRY_MAP = {
    "US": {"country": "US", "y2": "US02Y", "y10": "US10Y"},
    "CA": {"country": "CA", "y2": "CAST", "y10": "CA10Y"},

    "EU": {"country": "EU", "y2": None, "y10": "EU10Y"},
    "DE": {"country": "DE", "y2": "DEST", "y10": "DE10Y"},
    "FR": {"country": "FR", "y2": "FRST", "y10": "FR10Y"},
    "IT": {"country": "IT", "y2": "ITST", "y10": "IT10Y"},
    "ES": {"country": "ES", "y2": "ESST", "y10": "ES10Y"},
    "UK": {"country": "UK", "y2": "UKST", "y10": "UK10Y"},

    "JP": {"country": "JP", "y2": "JPST", "y10": "JP10Y"},
    "KR": {"country": "KR", "y2": "KRST", "y10": "KR10Y"},
    "AU": {"country": "AU", "y2": "AUST", "y10": "AU10Y"},
}


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv(R2_ENDPOINT_ENV),
        aws_access_key_id=os.getenv(R2_ACCESS_KEY_ID_ENV),
        aws_secret_access_key=os.getenv(R2_SECRET_ACCESS_KEY_ENV),
        region_name=os.getenv(R2_REGION_ENV),
    )


def read_monthly_leaf() -> pd.DataFrame:
    bucket = os.getenv(R2_BUCKET_ENV)
    if not bucket:
        raise ValueError(f"{R2_BUCKET_ENV} not set")

    obj = s3_client().get_object(
        Bucket=bucket,
        Key=R2_RATES_YIELDS_MONTHLY_T1_KEY,
    )

    df = pd.read_parquet(BytesIO(obj["Body"].read()))
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    return df.dropna(subset=["symbol", "date", "value"])


def build_panel(df: pd.DataFrame) -> pd.DataFrame:
    wide = (
        df.pivot_table(
            index="date",
            columns="symbol",
            values="value",
            aggfunc="last",
        )
        .sort_index()
        .ffill()
        .reset_index()
    )

    rows = []

    for country, cfg in COUNTRY_MAP.items():
        y2_col = cfg.get("y2")
        y10_col = cfg["y10"]

        has_y2 = bool(y2_col) and y2_col in wide.columns
        has_y10 = bool(y10_col) and y10_col in wide.columns

        if not has_y10:
            print(f"WARNING: missing {y10_col}; skipping {country}")
            continue

        temp = pd.DataFrame()
        temp["date"] = wide["date"]
        temp["country"] = country
        temp["series_type"] = "curve"

        temp["curve_eligible"] = has_y2
        temp["spread_eligible"] = has_y2

        temp["y2"] = wide[y2_col] if has_y2 else None
        temp["y10"] = wide[y10_col]
        temp["spread"] = temp["y10"] - temp["y2"] if has_y2 else None

        temp["symbol"] = f"{country}_CURVE"
        temp["y10_proxy"] = None
        temp["y10_proxy_source"] = y10_col

        temp["policy_proxy"] = None
        temp["policy_proxy_source"] = y2_col if has_y2 else None
        temp["policy_pressure_t1"] = None
        temp["state"] = None

        rows.append(temp)

    out = pd.concat(rows, ignore_index=True)
    out = out.dropna(subset=["date", "country", "y10"])
    out["date"] = pd.to_datetime(out["date"]).dt.strftime("%Y-%m-%d")

    return out.sort_values(["country", "date"]).reset_index(drop=True)


def write_json_to_r2(df: pd.DataFrame) -> None:
    bucket = os.getenv(R2_BUCKET_ENV)
    if not bucket:
        raise ValueError(f"{R2_BUCKET_ENV} not set")

    body = df.to_json(orient="records", indent=2).encode("utf-8")

    s3_client().put_object(
        Bucket=bucket,
        Key=OUT_KEY,
        Body=body,
        ContentType="application/json",
        CacheControl="no-store",
    )


def main() -> None:
    df = read_monthly_leaf()
    panel = build_panel(df)
    write_json_to_r2(panel)

    print("RATES selected global panel complete.")
    print(f"Rows: {len(panel)}")
    print(f"Countries: {sorted(panel['country'].unique())}")
    print(f"Latest date: {panel['date'].max()}")


if __name__ == "__main__":
    main()
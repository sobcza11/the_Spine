from __future__ import annotations

import io
import os
import sys
from datetime import datetime, timezone

import boto3
import pandas as pd

R2_KEY = "spine_us/us_equity_index_t1.parquet"
CANON_COLS = ["symbol", "date", "open", "high", "low", "close", "volume", "source"]
FRESHNESS_DAYS = 5


def _env(name: str, required: bool = True) -> str:
    v = os.getenv(name, "").strip()
    if required and not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def _bucket_name() -> str:
    return _env("R2_BUCKET_NAME", required=False) or _env("R2_BUCKET", required=True)


def _r2_endpoint() -> str:
    ep = _env("R2_ENDPOINT", required=False)
    if ep:
        return ep
    account_id = _env("R2_ACCOUNT_ID", True)
    return f"https://{account_id}.r2.cloudflarestorage.com"


def _r2_client():
    return boto3.client(
        "s3",
        endpoint_url=_r2_endpoint(),
        aws_access_key_id=_env("R2_ACCESS_KEY_ID", True),
        aws_secret_access_key=_env("R2_SECRET_ACCESS_KEY", True),
        region_name="auto",
    )


def _read_parquet_from_r2(key: str) -> pd.DataFrame:
    obj = _r2_client().get_object(Bucket=_bucket_name(), Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


def main() -> int:
    print("\n=== EQUITY INDEX VALIDATE (T1) ===")
    df = _read_parquet_from_r2(R2_KEY)

    missing = [c for c in CANON_COLS if c not in df.columns]
    if missing:
        raise SystemExit(f"Missing required cols: {missing}")

    if df.empty:
        raise SystemExit("Leaf is empty")

    df["symbol"] = df["symbol"].astype(str).str.upper()
    df["date"] = pd.to_datetime(df["date"]).dt.floor("D")

    if df.duplicated(["symbol", "date"]).any():
        raise SystemExit("Duplicate (symbol,date) rows detected")

    if df["symbol"].isna().any() or df["date"].isna().any():
        raise SystemExit("Nulls in key cols (symbol/date)")

    max_date = df["date"].max()
    age_days = (datetime.now(timezone.utc) - max_date.to_pydatetime().replace(tzinfo=timezone.utc)).days
    if age_days > FRESHNESS_DAYS:
        raise SystemExit(f"Leaf stale: max_date={max_date} age_days={age_days}")

    print("Equity index validation passed.")
    print("rows:", len(df))
    print("symbols:", int(df["symbol"].nunique()))
    print("max_date:", max_date)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise

    
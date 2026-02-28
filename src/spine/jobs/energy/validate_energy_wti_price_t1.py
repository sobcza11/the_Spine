import os
from io import BytesIO
from datetime import datetime, timedelta, UTC

import boto3
import botocore
import pandas as pd

from spine.jobs.energy.energy_constants import (
    R2_WTI_PRICE_T1_KEY,
    WTI_SYMBOL,
    WTI_MAX_LAG_DAYS,
    R2_ACCESS_KEY_ID_ENV,
    R2_SECRET_ACCESS_KEY_ENV,
    R2_BUCKET_ENV,
    R2_ENDPOINT_ENV,
    R2_REGION_ENV,
)

# ============================================================
# WTI PRICE T1 — VALIDATION
# NOTE: WTI can be negative (measurement must reflect reality).
# We enforce bounded sanity, not positivity.
# ============================================================

WTI_MIN_CLOSE = -200.0
WTI_MAX_CLOSE = 1000.0


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv(R2_ENDPOINT_ENV),
        aws_access_key_id=os.getenv(R2_ACCESS_KEY_ID_ENV),
        aws_secret_access_key=os.getenv(R2_SECRET_ACCESS_KEY_ENV),
        region_name=os.getenv(R2_REGION_ENV),
    )


def _read_leaf() -> pd.DataFrame:
    s3 = _s3_client()
    bucket = os.getenv(R2_BUCKET_ENV)
    if not bucket:
        raise ValueError("R2_BUCKET not set")

    try:
        obj = s3.get_object(Bucket=bucket, Key=R2_WTI_PRICE_T1_KEY)
        return pd.read_parquet(BytesIO(obj["Body"].read()))
    except botocore.exceptions.ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "404"):
            raise FileNotFoundError(f"Leaf not found in R2: {R2_WTI_PRICE_T1_KEY}") from e
        raise


def _require_cols(df: pd.DataFrame, cols: list[str]) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise KeyError(f"Missing required columns: {missing}. Found: {list(df.columns)}")


def main():
    df = _read_leaf().copy()

    # Schema
    _require_cols(df, ["symbol", "date", "close"])

    # Types
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    # Nulls
    if df["symbol"].isna().any():
        raise ValueError("Nulls found in symbol")
    if df["date"].isna().any():
        raise ValueError("Nulls/invalid found in date")
    if df["close"].isna().any():
        raise ValueError("Nulls/invalid found in close")

    # Symbol constraint
    bad_sym = df.loc[df["symbol"] != WTI_SYMBOL, "symbol"].unique().tolist()
    if bad_sym:
        raise ValueError(f"Unexpected symbols found (expected only {WTI_SYMBOL}): {bad_sym}")

    # Duplicates
    dupes = df.duplicated(subset=["symbol", "date"]).sum()
    if dupes:
        raise ValueError(f"Duplicate rows found on (symbol,date): {int(dupes)}")

    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)

    # Sanity bounds (allow negatives)
    if (df["close"] < WTI_MIN_CLOSE).any():
        mn = float(df["close"].min())
        raise ValueError(f"WTI close below sanity bound: min={mn} < {WTI_MIN_CLOSE}")
    if (df["close"] > WTI_MAX_CLOSE).any():
        mx = float(df["close"].max())
        raise ValueError(f"WTI close above sanity bound: max={mx} > {WTI_MAX_CLOSE}")

    # ------------------------------------------------------------
    # Freshness (allow publication lag) — timezone-safe
    # ------------------------------------------------------------
    last_dt = pd.to_datetime(df["date"].max())
    if pd.isna(last_dt):
        raise ValueError("Freshness check failed: last_dt is NaT")

    # Ensure last_dt is UTC-aware
    if last_dt.tzinfo is None:
        last_dt = last_dt.tz_localize("UTC")
    else:
        last_dt = last_dt.tz_convert("UTC")

    now_utc = datetime.now(UTC)

    last_py = last_dt.to_pydatetime()
    if (now_utc - last_py) > timedelta(days=int(WTI_MAX_LAG_DAYS)):
        lag_days = (now_utc - last_py).days
        raise ValueError(
            f"WTI T1 freshness failed. last_date={last_dt.date()} "
            f"now_utc={now_utc.date()} lag_days={lag_days} "
            f"allowed={WTI_MAX_LAG_DAYS}"
        )

    print("✅ validate_energy_wti_price_t1 PASSED")
    print(f"Rows: {len(df)} | Last date: {last_dt.date()}")


if __name__ == "__main__":
    main()
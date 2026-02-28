import os
from io import BytesIO
from datetime import datetime, timedelta

import boto3
import botocore
import pandas as pd

from spine.jobs.energy.energy_constants import (
    R2_ACCESS_KEY_ID_ENV,
    R2_SECRET_ACCESS_KEY_ENV,
    R2_BUCKET_ENV,
    R2_ENDPOINT_ENV,
    R2_REGION_ENV,
)

# ============================================================
# INVENTORY T2 — VALIDATION
# Leaf: spine_us/leaves/energy/crude_stocks_ex_spr_t2.parquet
# ============================================================

CRUDE_STOCKS_EX_SPR_SYMBOL = "WCESTUS1"
R2_CRUDE_STOCKS_EX_SPR_T2_KEY = "spine_us/leaves/energy/crude_stocks_ex_spr_t2.parquet"

# Weekly release cadence tolerance (EIA weekly series can shift)
MAX_LAG_DAYS = 10
CADENCE_LOOKBACK_POINTS = 30
MIN_DIFF_DAYS = 5
MAX_DIFF_DAYS = 14


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
        obj = s3.get_object(Bucket=bucket, Key=R2_CRUDE_STOCKS_EX_SPR_T2_KEY)
        return pd.read_parquet(BytesIO(obj["Body"].read()))
    except botocore.exceptions.ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "404"):
            raise FileNotFoundError(f"Leaf not found in R2: {R2_CRUDE_STOCKS_EX_SPR_T2_KEY}") from e
        raise


def _require_cols(df: pd.DataFrame, cols: list[str]) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise KeyError(f"Missing required columns: {missing}. Found: {list(df.columns)}")


def _cadence_check(dates: pd.Series) -> None:
    d = pd.to_datetime(dates).sort_values().dropna()
    if len(d) < 10:
        return

    tail = d.iloc[-CADENCE_LOOKBACK_POINTS:] if len(d) > CADENCE_LOOKBACK_POINTS else d
    diffs = tail.diff().dropna().dt.days

    if diffs.empty:
        return

    # Hard bounds: flag obvious cadence corruption
    if (diffs < MIN_DIFF_DAYS).any():
        raise ValueError(f"Cadence check failed: found diffs < {MIN_DIFF_DAYS} days in weekly series")
    if (diffs > MAX_DIFF_DAYS).any():
        raise ValueError(f"Cadence check failed: found diffs > {MAX_DIFF_DAYS} days in weekly series")


def main():
    df = _read_leaf().copy()

    # -----------------------------
    # Schema
    # -----------------------------
    _require_cols(df, ["symbol", "date", "value"])

    # -----------------------------
    # Types
    # -----------------------------
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # -----------------------------
    # Nulls
    # -----------------------------
    if df["symbol"].isna().any():
        raise ValueError("Nulls found in symbol")
    if df["date"].isna().any():
        raise ValueError("Nulls/invalid found in date")
    if df["value"].isna().any():
        raise ValueError("Nulls/invalid found in value")

    # -----------------------------
    # Symbol constraint
    # -----------------------------
    bad_sym = df.loc[df["symbol"] != CRUDE_STOCKS_EX_SPR_SYMBOL, "symbol"].unique().tolist()
    if bad_sym:
        raise ValueError(f"Unexpected symbols found (expected only {CRUDE_STOCKS_EX_SPR_SYMBOL}): {bad_sym}")

    # -----------------------------
    # Duplicates
    # -----------------------------
    dupes = df.duplicated(subset=["symbol", "date"]).sum()
    if dupes:
        raise ValueError(f"Duplicate rows found on (symbol,date): {int(dupes)}")

    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)

    # -----------------------------
    # Freshness (weekly)
    # -----------------------------
    last_dt = pd.to_datetime(df["date"].max()).to_pydatetime()
    now_utc = datetime.utcnow()
    if (now_utc - last_dt) > timedelta(days=int(MAX_LAG_DAYS)):
        raise ValueError(
            f"Inventory T2 freshness failed. last_date={last_dt.date()} "
            f"now_utc={now_utc.date()} lag_days={(now_utc - last_dt).days} allowed={MAX_LAG_DAYS}"
        )

    # -----------------------------
    # Cadence check (weekly spacing)
    # -----------------------------
    _cadence_check(df["date"])

    print("✅ validate_energy_crude_stocks_ex_spr_t2 PASSED")
    print(f"Rows: {len(df)} | Last date: {last_dt.date()}")


if __name__ == "__main__":
    main()

    
"""
FX Spot Canonical (T1) — Validator

Validates the Tier-1 FX canonical leaf stored in Cloudflare R2.

Checks:
- Required columns exist
- (symbol, date) uniqueness
- OHLC null thresholds
- Freshness (max date within allowed lag)
- Minimum rows per symbol
- Universe integrity (strict)
"""

from __future__ import annotations

import io
import os
import sys
from datetime import datetime, timezone
from typing import List, Tuple

import boto3
import pandas as pd


# -----------------------------
# Config
# -----------------------------
T1_PAIRS: List[str] = ["EURUSD", "USDJPY", "GBPUSD", "USDCHF", "USDCNH"]

R2_KEY_CANONICAL = "spine_us/us_fx_spot_canonical_t1.parquet"

REQUIRED_COLS = ["symbol", "date", "open", "high", "low", "close", "source"]
OPTIONAL_COLS = ["mid", "bid", "ask", "volume"]
PRICE_COLS = ["open", "high", "low", "close"]

MAX_NULL_PCT_OHLC = 0.01
ALLOWED_LAG_DAYS = int(os.getenv("FX_ALLOWED_LAG_DAYS", "5"))
MIN_ROWS_PER_SYMBOL = int(os.getenv("FX_MIN_ROWS_PER_SYMBOL", "250"))


# -----------------------------
# Helpers
# -----------------------------
def _env(name: str, required: bool = True) -> str:
    v = os.getenv(name, "").strip()
    if required and not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def _r2_client():
    account_id = _env("R2_ACCOUNT_ID", True)
    access_key = _env("R2_ACCESS_KEY_ID", True)
    secret_key = _env("R2_SECRET_ACCESS_KEY", True)
    endpoint_url = f"https://{account_id}.r2.cloudflarestorage.com"

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="auto",
    )


def _read_parquet_from_r2(key: str) -> pd.DataFrame:
    bucket = _env("R2_BUCKET", True)
    s3 = _r2_client()
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


def _utc_today_date() -> pd.Timestamp:
    return pd.Timestamp(datetime.now(timezone.utc).date())


# -----------------------------
# Main
# -----------------------------
def main() -> int:
    df = _read_parquet_from_r2(R2_KEY_CANONICAL)
    if df.empty:
        raise RuntimeError("Canonical leaf is empty.")

    # accept legacy & normalize (belt & suspenders)
    if "symbol" not in df.columns and "pair" in df.columns:
        df = df.rename(columns={"pair": "symbol"})
    for c in OPTIONAL_COLS:
        if c not in df.columns:
            df[c] = pd.NA

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise RuntimeError(f"Missing required columns: {missing}. Found: {list(df.columns)}")

    df = df.copy()
    df["symbol"] = df["symbol"].astype(str).str.upper()
    df["date"] = pd.to_datetime(df["date"]).dt.floor("D")

    have = set(df["symbol"].unique().tolist())
    want = set([s.upper() for s in T1_PAIRS])
    miss = sorted(list(want - have))
    extra = sorted(list(have - want))
    if miss:
        raise RuntimeError(f"Canonical leaf missing expected T1 symbols: {miss}")
    if extra:
        raise RuntimeError(f"Canonical leaf contains unexpected symbols (not in T1): {extra}")

    dup = int(df.duplicated(["symbol", "date"]).sum())
    if dup:
        raise RuntimeError(f"Found duplicate (symbol, date) rows: {dup}")

    for c in PRICE_COLS:
        null_pct = float(df[c].isna().mean())
        if null_pct > MAX_NULL_PCT_OHLC:
            raise RuntimeError(f"Null pct too high for '{c}': {null_pct:.4f} > {MAX_NULL_PCT_OHLC:.4f}")

    max_d = df["date"].max()
    lag_days = int((_utc_today_date() - max_d).days)
    if lag_days > ALLOWED_LAG_DAYS:
        raise RuntimeError(f"Freshness fail: max_date={max_d.date()} lag_days={lag_days} > allowed={ALLOWED_LAG_DAYS}")

    counts = df.groupby("symbol")["date"].count()
    bad: List[Tuple[str, int]] = [(k, int(v)) for k, v in counts.items() if int(v) < MIN_ROWS_PER_SYMBOL]
    if bad:
        raise RuntimeError(f"Coverage fail: symbols below MIN_ROWS_PER_SYMBOL={MIN_ROWS_PER_SYMBOL}: {bad}")

    print("FX T1 VALIDATION PASSED")
    print("r2_key:", R2_KEY_CANONICAL)
    print("rows:", len(df))
    print("symbols:", int(df["symbol"].nunique()))
    print("min_date:", df["date"].min())
    print("max_date:", df["date"].max())
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise


    
"""
FX Spot Canonical (T1) — HIST builder (Tiingo)

Pull full available history (Tiingo constraint typically 2020+) for Tier-1 FX pairs,
normalize schema, de-duplicate, & write canonical parquet to Cloudflare R2.

T1 pairs:
EURUSD, USDJPY, GBPUSD, USDCHF, USDCNH

Env required:
- TIINGO_API_KEY
- R2_ACCOUNT_ID
- R2_ACCESS_KEY_ID
- R2_SECRET_ACCESS_KEY
- R2_BUCKET

Optional:
- FX_START_DATE (default 2020-01-01; floored at 2020-01-01)
- FX_END_DATE   (default None)
"""

from __future__ import annotations

import io
import os
import sys
import time
from typing import Any, Dict, List

import boto3
import pandas as pd
import requests


# -----------------------------
# Config
# -----------------------------
T1_PAIRS: List[str] = ["EURUSD", "USDJPY", "GBPUSD", "USDCHF", "USDCNH"]

R2_FX_SPOT_T1_KEY = "spine_us/us_fx_spot_canonical_t1.parquet"

TIINGO_BASE = "https://api.tiingo.com/tiingo/fx"

START_FLOOR = "2020-01-01"
RESAMPLE_FREQ = "1day"
RESAMPLE_LABEL = "1Day"

CANON_COLS = [
    "symbol",
    "date",
    "open",
    "high",
    "low",
    "close",
    "mid",
    "bid",
    "ask",
    "volume",
    "source",
]


# -----------------------------
# ENV + R2 Helpers (ENV-BASED ONLY)
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


def _upload_parquet_to_r2(df: pd.DataFrame, key: str) -> None:
    bucket = _env("R2_BUCKET", True)
    s3 = _r2_client()

    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)

    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())


# -----------------------------
# Tiingo Helpers
# -----------------------------
def _tiingo_headers() -> Dict[str, str]:
    token = _env("TIINGO_API_KEY", True)
    return {"Authorization": f"Token {token}"}


def _fetch_tiingo_fx_prices(
    ticker: str,
    start_date: str,
    end_date: str | None = None,
    resample_freq: str = RESAMPLE_FREQ,
) -> pd.DataFrame:
    params: Dict[str, Any] = {"startDate": start_date, "resampleFreq": resample_freq}
    if end_date:
        params["endDate"] = end_date

    url = f"{TIINGO_BASE}/{ticker.lower()}/prices"

    for attempt in range(6):
        r = requests.get(url, headers=_tiingo_headers(), params=params, timeout=60)

        if r.status_code == 200:
            data = r.json()
            return pd.DataFrame(data) if data else pd.DataFrame()

        if r.status_code == 429:
            sleep_s = int(2**attempt)  # 1,2,4,8,16,32
            print(f"Tiingo 429 rate limit for {ticker}; backoff {sleep_s}s (attempt {attempt+1}/6)")
            time.sleep(sleep_s)
            continue

        raise RuntimeError(f"Tiingo request failed ({ticker}) {r.status_code}: {r.text[:500]}")

    raise RuntimeError(f"Tiingo request failed ({ticker}) 429: rate limit exceeded after retries")


def _normalize_fx_schema(df_raw: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if df_raw.empty:
        return df_raw

    df = df_raw.copy()
    if "date" not in df.columns:
        raise KeyError(f"Tiingo payload missing 'date' for {symbol}. Found: {list(df.columns)}")

    df["date"] = pd.to_datetime(df["date"], utc=True).dt.tz_convert(None).dt.floor("D")
    df["symbol"] = symbol.upper()
    df["source"] = "tiingo"

    rename_map = {"bidPrice": "bid", "askPrice": "ask", "midPrice": "mid"}
    for k, v in rename_map.items():
        if k in df.columns and v not in df.columns:
            df = df.rename(columns={k: v})

    for c in CANON_COLS:
        if c not in df.columns:
            df[c] = pd.NA

    df = df[CANON_COLS]

    for c in ["open", "high", "low", "close", "mid", "bid", "ask", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    return df


# -----------------------------
# Main
# -----------------------------
def main() -> int:
    start_date = os.getenv("FX_START_DATE", "").strip() or START_FLOOR
    if pd.Timestamp(start_date) < pd.Timestamp(START_FLOOR):
        start_date = START_FLOOR

    end_date = os.getenv("FX_END_DATE", "").strip() or None

    print("\n=== FX Spot HIST (Tier 1 | Tiingo) ===")
    print(f"R2 key={R2_FX_SPOT_T1_KEY}")
    print(f"startFloor={START_FLOOR} | startDate={start_date} | endDate={end_date or 'None'} | resampleFreq={RESAMPLE_LABEL}")
    print(f"pairs={T1_PAIRS}")

    frames: List[pd.DataFrame] = []
    for sym in T1_PAIRS:
        raw = _fetch_tiingo_fx_prices(sym, start_date=start_date, end_date=end_date, resample_freq=RESAMPLE_FREQ)
        norm = _normalize_fx_schema(raw, symbol=sym)
        if not norm.empty:
            frames.append(norm)

    if not frames:
        raise RuntimeError("No FX data returned for T1 universe; refusing to write empty canonical leaf.")

    df = pd.concat(frames, ignore_index=True)
    df = df.sort_values(["symbol", "date"]).drop_duplicates(["symbol", "date"], keep="last").reset_index(drop=True)

    _upload_parquet_to_r2(df, R2_FX_SPOT_T1_KEY)

    print("\nFX T1 HIST build complete")
    print("rows:", len(df))
    print("symbols:", int(df["symbol"].nunique()))
    print("min_date:", df["date"].min())
    print("max_date:", df["date"].max())
    print("r2_key:", R2_FX_SPOT_T1_KEY)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise

    
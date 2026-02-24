"""
FX Spot Canonical (T2) — UPDATE builder (Tiingo)

Reads existing canonical leaf from R2, pulls only new dates from Tiingo for T2 pairs,
appends, de-dupes, rewrites leaf.

Env required:
- TIINGO_API_KEY
- R2_ACCOUNT_ID
- R2_ACCESS_KEY_ID
- R2_SECRET_ACCESS_KEY
- R2_BUCKET

Optional:
- FX_END_DATE (default None)
"""

from __future__ import annotations

import io
import os
import sys
import time
from datetime import timedelta
from typing import Any, Dict, List

import boto3
import pandas as pd
import requests


T2_PAIRS: List[str] = [
    "USDCAD",
    "AUDUSD",
    "NZDUSD",
    "EURJPY",
    "AUDJPY",
    "USDSEK",
    "USDNOK",
    "USDMXN",
    "USDZAR",
]

R2_FX_SPOT_T2_KEY = "spine_us/us_fx_spot_canonical_t2.parquet"

TIINGO_BASE = "https://api.tiingo.com/tiingo/fx"

START_FLOOR = "2020-01-01"
RESAMPLE_FREQ = "1day"
RESAMPLE_LABEL = "1Day"

OVERLAP_DAYS = 5

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


def _upload_parquet_to_r2(df: pd.DataFrame, key: str) -> None:
    bucket = _env("R2_BUCKET", True)
    s3 = _r2_client()

    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)

    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())


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
            sleep_s = int(2**attempt)
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


def _compute_start_dates(df_leaf: pd.DataFrame) -> Dict[str, str]:
    starts: Dict[str, str] = {}
    floor = pd.Timestamp(START_FLOOR)

    if df_leaf.empty:
        for sym in T2_PAIRS:
            starts[sym] = START_FLOOR
        return starts

    dfx = df_leaf.copy()
    dfx["symbol"] = dfx["symbol"].astype(str).str.upper()
    dfx["date"] = pd.to_datetime(dfx["date"]).dt.floor("D")

    for sym in T2_PAIRS:
        sub = dfx[dfx["symbol"] == sym]
        if sub.empty:
            starts[sym] = START_FLOOR
        else:
            max_dt = sub["date"].max()
            start_dt = max_dt - timedelta(days=OVERLAP_DAYS)
            if start_dt < floor:
                start_dt = floor
            starts[sym] = start_dt.date().isoformat()

    return starts


def main() -> int:
    end_date = os.getenv("FX_END_DATE", "").strip() or None

    print("\n=== FX Spot UPDATE (Tier 2 | Tiingo) ===")
    print(f"R2 key={R2_FX_SPOT_T2_KEY}")
    print(f"startFloor={START_FLOOR} | endDate={end_date or 'None'} | resampleFreq={RESAMPLE_LABEL}")
    print(f"pairs={T2_PAIRS}")

    df_existing = _read_parquet_from_r2(R2_FX_SPOT_T2_KEY)
    if df_existing.empty:
        df_existing = pd.DataFrame(columns=CANON_COLS)

    # normalize existing schema
    if "symbol" not in df_existing.columns and "pair" in df_existing.columns:
        df_existing = df_existing.rename(columns={"pair": "symbol"})
    for c in ["mid", "bid", "ask", "volume"]:
        if c not in df_existing.columns:
            df_existing[c] = pd.NA
    df_existing["symbol"] = df_existing["symbol"].astype(str).str.upper() if "symbol" in df_existing.columns else df_existing.get("symbol")
    df_existing["date"] = pd.to_datetime(df_existing["date"]).dt.floor("D") if "date" in df_existing.columns else df_existing.get("date")
    df_existing = df_existing[CANON_COLS] if set(CANON_COLS).issubset(set(df_existing.columns)) else df_existing

    starts = _compute_start_dates(df_existing)

    frames: List[pd.DataFrame] = []
    for sym in T2_PAIRS:
        raw = _fetch_tiingo_fx_prices(sym, start_date=starts[sym], end_date=end_date, resample_freq=RESAMPLE_FREQ)
        norm = _normalize_fx_schema(raw, symbol=sym)
        if not norm.empty:
            frames.append(norm)

    if not frames:
        print("No new data returned from Tiingo; leaving canonical leaf unchanged.")
        print("r2_key:", R2_FX_SPOT_T2_KEY)
        return 0

    df_new = pd.concat(frames, ignore_index=True)

    df_out = pd.concat([df_existing, df_new], ignore_index=True) if not df_existing.empty else df_new
    df_out["symbol"] = df_out["symbol"].astype(str).str.upper()
    df_out["date"] = pd.to_datetime(df_out["date"]).dt.floor("D")
    df_out = df_out.sort_values(["symbol", "date"]).drop_duplicates(["symbol", "date"], keep="last").reset_index(drop=True)

    _upload_parquet_to_r2(df_out, R2_FX_SPOT_T2_KEY)

    print("\nFX T2 UPDATE complete")
    print("rows:", len(df_out))
    print("symbols:", int(df_out["symbol"].nunique()))
    print("max_date:", df_out["date"].max())
    print("r2_key:", R2_FX_SPOT_T2_KEY)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise
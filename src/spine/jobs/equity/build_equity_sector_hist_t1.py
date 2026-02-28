from __future__ import annotations

import io
import os
import sys
import time
from typing import Dict, List, Optional

import boto3
import pandas as pd
import requests

T1_SECTOR_ETFS: List[str] = ["XLB", "XLC", "XLE", "XLF", "XLI", "XLK", "XLP", "XLRE", "XLU", "XLV", "XLY"]

R2_KEY = "spine_us/us_sector_etf_t1.parquet"

START_FLOOR = "2000-01-01"
CANON_COLS = ["symbol", "date", "open", "high", "low", "close", "volume", "source"]

TIINGO_DAILY_BASE = "https://api.tiingo.com/tiingo/daily"


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


def _upload_parquet_to_r2(df: pd.DataFrame, key: str) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    _r2_client().put_object(Bucket=_bucket_name(), Key=key, Body=buf.getvalue())


def _tiingo_headers() -> Dict[str, str]:
    return {"Authorization": f"Token {_env('TIINGO_API_KEY', True)}"}


def _fetch_tiingo_daily(symbol: str, start_date: str, end_date: Optional[str]) -> pd.DataFrame:
    url = f"https://api.tiingo.com/tiingo/daily/{symbol}/prices"
    params: Dict[str, str] = {"startDate": start_date}
    if end_date:
        params["endDate"] = end_date

    retries = 6
    backoff = 1
    for _ in range(retries):
        r = requests.get(url, headers=_tiingo_headers(), params=params, timeout=30)
        if r.status_code == 429:
            time.sleep(backoff)
            backoff *= 2
            continue
        r.raise_for_status()
        return pd.DataFrame(r.json())

    raise RuntimeError(f"Tiingo 429 persisted after retries for {symbol}")


def _normalize_equity_schema(df_raw: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if df_raw is None or df_raw.empty:
        return pd.DataFrame(columns=CANON_COLS)

    df = df_raw.copy()
    if "date" not in df.columns:
        raise KeyError(f"Tiingo payload missing 'date' for {symbol}. Found: {list(df.columns)}")

    df["date"] = pd.to_datetime(df["date"], utc=True).dt.tz_convert(None).dt.floor("D")
    df["symbol"] = symbol.upper()
    df["source"] = "tiingo"

    for c in ["open", "high", "low", "close", "volume"]:
        if c not in df.columns:
            df[c] = pd.NA
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df[CANON_COLS]
    return df


def _dedupe_sort(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["symbol"] = out["symbol"].astype(str).str.upper()
    out["date"] = pd.to_datetime(out["date"]).dt.floor("D")
    out = (
        out.sort_values(["symbol", "date"])
        .drop_duplicates(["symbol", "date"], keep="last")
        .reset_index(drop=True)
    )
    return out


def main() -> int:
    end_date = os.getenv("EQ_END_DATE", "").strip() or None

    print("\n=== EQUITY SECTOR HIST (T1 | Tiingo) ===")
    print(f"R2 key={R2_KEY}")
    print(f"startFloor={START_FLOOR} | endDate={end_date or 'None'}")
    print(f"tickers={T1_SECTOR_ETFS}")

    frames: List[pd.DataFrame] = []
    for sym in T1_SECTOR_ETFS:
        raw = _fetch_tiingo_daily(sym, start_date=START_FLOOR, end_date=end_date)
        norm = _normalize_equity_schema(raw, symbol=sym)
        if not norm.empty:
            frames.append(norm)

    if not frames:
        raise SystemExit("No data returned from Tiingo for HIST build")

    df_out = _dedupe_sort(pd.concat(frames, ignore_index=True))
    _upload_parquet_to_r2(df_out, R2_KEY)

    print("\nEQUITY SECTOR T1 HIST complete")
    print("rows:", len(df_out))
    print("symbols:", int(df_out["symbol"].nunique()))
    print("max_date:", df_out["date"].max())
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise

    
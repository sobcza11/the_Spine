from __future__ import annotations

import io
import os
import sys
import time
from datetime import timedelta
from typing import Dict, List, Optional

import boto3
import pandas as pd
import requests

VINV_TICKERS: List[str] = sorted(
    set(
        [
            "AAPL",
            "AFL",
            "AJG",
            "ADP",
            "AVT",
            "BDX",
            "BOH",
            "BRO",
            "CASY",
            "CL",
            "CLX",
            "CVX",
            "DCI",
            "DE",
            "DOV",
            "ECL",
            "ED",
            "FCNCA",
            "GPC",
            "GWW",
            "HES",
            "HUBB",
            "JNJ",
            "K",
            "KO",
            "KR",
            "LEG",
            "LLY",
            "MCD",
            "MTB",
            "NDSN",
            "NNN",
            "OKE",
            "OMC",
            "PEG",
            "PH",
            "PNW",
            "PPG",
            "RHI",
            "RRX",
            "SEE",
            "SHW",
            "SNA",
            "SO",
            "TFX",
            "TGT",
            "UGI",
            "VFC",
            "WBA",
            "WHR",
            "WMT",
            "XOM",
        ]
    )
)

R2_KEY = "spine_us/us_vinv_components.parquet"

START_FLOOR = "2000-01-01"
OVERLAP_DAYS = 5

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


def _read_parquet_from_r2(key: str) -> pd.DataFrame:
    obj = _r2_client().get_object(Bucket=_bucket_name(), Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


def _upload_parquet_to_r2(df: pd.DataFrame, key: str) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    _r2_client().put_object(Bucket=_bucket_name(), Key=key, Body=buf.getvalue())


def _tiingo_headers() -> Dict[str, str]:
    return {"Authorization": f"Token {_env('TIINGO_API_KEY', True)}"}


def _fetch_tiingo_daily(symbol: str, start_date: str, end_date: Optional[str]) -> pd.DataFrame:
    url = f"{TIINGO_DAILY_BASE}/{symbol}/prices"
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


def _compute_start_dates(df_leaf: pd.DataFrame) -> Dict[str, str]:
    floor = pd.Timestamp(START_FLOOR)

    if df_leaf is None or df_leaf.empty:
        return {s: START_FLOOR for s in VINV_TICKERS}

    dfx = df_leaf.copy()
    dfx["symbol"] = dfx["symbol"].astype(str).str.upper()
    dfx["date"] = pd.to_datetime(dfx["date"]).dt.floor("D")

    out: Dict[str, str] = {}
    for sym in VINV_TICKERS:
        sub = dfx[dfx["symbol"] == sym]
        if sub.empty:
            out[sym] = START_FLOOR
            continue
        max_dt = sub["date"].max()
        start_dt = max_dt - timedelta(days=OVERLAP_DAYS)
        if start_dt < floor:
            start_dt = floor
        out[sym] = start_dt.date().isoformat()

    return out


def main() -> int:
    end_date = os.getenv("EQ_END_DATE", "").strip() or None

    print("\n=== VINV COMPONENTS UPDATE (T3 | Tiingo) ===")
    print(f"R2 key={R2_KEY}")
    print(f"startFloor={START_FLOOR} | endDate={end_date or 'None'}")
    print(f"tickers={VINV_TICKERS}")

    df_existing = _read_parquet_from_r2(R2_KEY)
    if df_existing is None or df_existing.empty:
        df_existing = pd.DataFrame(columns=CANON_COLS)

    for c in CANON_COLS:
        if c not in df_existing.columns:
            df_existing[c] = pd.NA
    df_existing = df_existing[CANON_COLS]
    df_existing["symbol"] = df_existing["symbol"].astype(str).str.upper()
    df_existing["date"] = pd.to_datetime(df_existing["date"]).dt.floor("D")

    starts = _compute_start_dates(df_existing)

    frames: List[pd.DataFrame] = []
    for sym in VINV_TICKERS:
        raw = _fetch_tiingo_daily(sym, start_date=starts[sym], end_date=end_date)
        norm = _normalize_equity_schema(raw, symbol=sym)
        if not norm.empty:
            frames.append(norm)

    if not frames:
        print("No new data returned from Tiingo; leaving components leaf unchanged.")
        print("r2_key:", R2_KEY)
        return 0

    df_new = pd.concat(frames, ignore_index=True)
    df_out = _dedupe_sort(pd.concat([df_existing, df_new], ignore_index=True))

    _upload_parquet_to_r2(df_out, R2_KEY)

    print("\nVINV COMPONENTS UPDATE complete")
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
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

GLOBAL_REGION_ETFS = {
    "VGK": "Europe+",
    "EUFN": "Europe+",
    "EXI": "Europe+",
    "AAXJ": "Asia-Pacific",
    "EWJ": "Japan",
    "EWA": "Australia",
    "EWH": "Hong Kong",
    "FXI": "China Gateway",
}

R2_KEY = "spine_us/global_equity_region_t1.parquet"

START_FLOOR = "2000-01-01"
OVERLAP_DAYS = 5

CANON_COLS = [
    "symbol",
    "region",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "source",
]


def _env(name: str, required: bool = True) -> str:
    v = os.getenv(name, "").strip()
    if required and not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


def _bucket_name() -> str:
    return _env("R2_BUCKET_NAME", False) or _env("R2_BUCKET")


def _r2_endpoint() -> str:
    ep = _env("R2_ENDPOINT", False)
    if ep:
        return ep

    account_id = _env("R2_ACCOUNT_ID")
    return f"https://{account_id}.r2.cloudflarestorage.com"


def _r2_client():
    return boto3.client(
        "s3",
        endpoint_url=_r2_endpoint(),
        aws_access_key_id=_env("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=_env("R2_SECRET_ACCESS_KEY"),
        region_name="auto",
    )


def _read_parquet_from_r2(key: str) -> pd.DataFrame:
    obj = _r2_client().get_object(
        Bucket=_bucket_name(),
        Key=key
    )
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


def _upload_parquet_to_r2(df: pd.DataFrame, key: str) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)

    _r2_client().put_object(
        Bucket=_bucket_name(),
        Key=key,
        Body=buf.getvalue(),
    )


def _tiingo_headers():
    return {
        "Authorization": f"Token {_env('TIINGO_API_KEY')}"
    }


def _fetch_tiingo_daily(
    symbol: str,
    start_date: str,
    end_date: Optional[str]
) -> pd.DataFrame:

    url = f"https://api.tiingo.com/tiingo/daily/{symbol}/prices"

    params = {"startDate": start_date}

    if end_date:
        params["endDate"] = end_date

    retries = 6
    backoff = 1

    for _ in range(retries):

        r = requests.get(
            url,
            headers=_tiingo_headers(),
            params=params,
            timeout=30,
        )

        if r.status_code == 429:
            time.sleep(backoff)
            backoff *= 2
            continue

        r.raise_for_status()

        return pd.DataFrame(r.json())

    raise RuntimeError(f"429 persisted: {symbol}")


def _normalize(
    df_raw: pd.DataFrame,
    symbol: str,
    region: str
) -> pd.DataFrame:

    if df_raw is None or df_raw.empty:
        return pd.DataFrame(columns=CANON_COLS)

    df = df_raw.copy()

    df["date"] = (
        pd.to_datetime(df["date"], utc=True)
        .dt.tz_convert(None)
        .dt.floor("D")
    )

    df["symbol"] = symbol
    df["region"] = region
    df["source"] = "tiingo"

    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    return df[CANON_COLS]


def _compute_start_dates(
    df_existing: pd.DataFrame
) -> Dict[str, str]:

    if df_existing.empty:
        return {
            s: START_FLOOR
            for s in GLOBAL_REGION_ETFS
        }

    out = {}

    for sym in GLOBAL_REGION_ETFS:

        sub = df_existing[
            df_existing["symbol"] == sym
        ]

        if sub.empty:
            out[sym] = START_FLOOR
            continue

        start_dt = (
            sub["date"].max()
            - timedelta(days=OVERLAP_DAYS)
        )

        out[sym] = start_dt.date().isoformat()

    return out


def main():

    print("\n=== GLOBAL EQUITY REGION UPDATE ===")
    print(f"R2 key: {R2_KEY}")

    try:
        df_existing = _read_parquet_from_r2(R2_KEY)
    except Exception:
        df_existing = pd.DataFrame(columns=CANON_COLS)

    starts = _compute_start_dates(df_existing)

    frames = []

    for symbol, region in GLOBAL_REGION_ETFS.items():

        raw = _fetch_tiingo_daily(
            symbol=symbol,
            start_date=starts[symbol],
            end_date=None,
        )

        norm = _normalize(
            raw,
            symbol,
            region
        )

        if not norm.empty:
            frames.append(norm)

    if not frames:
        return 0

    df_new = pd.concat(
        frames,
        ignore_index=True
    )

    df_out = (
        pd.concat(
            [df_existing, df_new],
            ignore_index=True
        )
        .sort_values(["symbol", "date"])
        .drop_duplicates(
            ["symbol", "date"],
            keep="last"
        )
    )

    _upload_parquet_to_r2(
        df_out,
        R2_KEY
    )

    print("rows:", len(df_out))
    print("symbols:", df_out["symbol"].nunique())
    print("max_date:", df_out["date"].max())

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

    
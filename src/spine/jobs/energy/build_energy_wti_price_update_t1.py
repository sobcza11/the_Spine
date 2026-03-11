
import os
from io import BytesIO
from datetime import datetime, timedelta, UTC

import boto3
import botocore
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import spine.jobs.energy.energy_constants as ec

WTI_EIA_SERIES_ID = getattr(
    ec,
    "WTI_EIA_SERIES_ID",
    getattr(
        ec,
        "EIA_WTI_SERIES_ID",
        getattr(ec, "WTI_SERIES_ID", "PET.RWTC.D"),
    ),
)

WTI_SYMBOL = ec.WTI_SYMBOL
R2_WTI_PRICE_T1_KEY = ec.R2_WTI_PRICE_T1_KEY
WTI_MAX_LAG_DAYS = getattr(ec, "WTI_MAX_LAG_DAYS", 10)
EIA_API_KEY_ENV = ec.EIA_API_KEY_ENV
R2_ACCOUNT_ID_ENV = ec.R2_ACCOUNT_ID_ENV
R2_ACCESS_KEY_ID_ENV = ec.R2_ACCESS_KEY_ID_ENV
R2_SECRET_ACCESS_KEY_ENV = ec.R2_SECRET_ACCESS_KEY_ENV
R2_BUCKET_ENV = ec.R2_BUCKET_ENV
R2_BUCKET_NAME_ENV = ec.R2_BUCKET_NAME_ENV
R2_ENDPOINT_ENV = ec.R2_ENDPOINT_ENV
R2_REGION_ENV = ec.R2_REGION_ENV


# ----------------------------
# Networking controls (CPMAI)
# ----------------------------
HTTP_CONNECT_TIMEOUT_S = 10
HTTP_READ_TIMEOUT_S = 90
HTTP_RETRIES_TOTAL = 6
HTTP_BACKOFF_FACTOR = 1.2
HTTP_STATUS_FORCELIST = (429, 500, 502, 503, 504)

# ----------------------------
# Deterministic overlap fetch
# ----------------------------
OVERLAP_DAYS = 14  # re-pull last N days to catch late EIA publications


def _requests_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=HTTP_RETRIES_TOTAL,
        connect=HTTP_RETRIES_TOTAL,
        read=HTTP_RETRIES_TOTAL,
        status=HTTP_RETRIES_TOTAL,
        backoff_factor=HTTP_BACKOFF_FACTOR,
        status_forcelist=HTTP_STATUS_FORCELIST,
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    return s


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv(R2_ENDPOINT_ENV),
        aws_access_key_id=os.getenv(R2_ACCESS_KEY_ID_ENV),
        aws_secret_access_key=os.getenv(R2_SECRET_ACCESS_KEY_ENV),
        region_name=os.getenv(R2_REGION_ENV),
    )


def read_existing_leaf() -> pd.DataFrame:
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
            return pd.DataFrame(columns=["symbol", "date", "close"])
        raise


def write_leaf(df: pd.DataFrame) -> None:
    s3 = _s3_client()
    bucket = os.getenv(R2_BUCKET_ENV)
    if not bucket:
        raise ValueError("R2_BUCKET not set")

    buf = BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    s3.put_object(Bucket=bucket, Key=R2_WTI_PRICE_T1_KEY, Body=buf.getvalue())


def _eia_url(start_date: str) -> str:
    api_key = os.getenv(EIA_API_KEY_ENV)
    if not api_key:
        raise ValueError("EIA_API_KEY not set")

    return (
        "https://api.eia.gov/v2/seriesid/"
        f"{WTI_EIA_SERIES_ID}?api_key={api_key}"
        f"&out=json&start={start_date}&sort[0][column]=period&sort[0][direction]=asc"
    )


def fetch_incremental(start_date: str) -> pd.DataFrame:
    url = _eia_url(start_date)
    s = _requests_session()

    r = s.get(url, timeout=(HTTP_CONNECT_TIMEOUT_S, HTTP_READ_TIMEOUT_S))
    r.raise_for_status()

    js = r.json()
    data = js.get("response", {}).get("data", [])
    if not data:
        return pd.DataFrame(columns=["symbol", "date", "close"])

    df = pd.DataFrame(data)
    if "period" not in df.columns or "value" not in df.columns:
        raise ValueError(f"Unexpected EIA payload columns: {list(df.columns)}")

    df = df.rename(columns={"period": "date", "value": "close"})
    df["symbol"] = WTI_SYMBOL
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    df = df.dropna(subset=["symbol", "date", "close"])
    df = (
        df.sort_values(["symbol", "date"])
        .drop_duplicates(["symbol", "date"], keep="last")
        .reset_index(drop=True)
    )

    return df[["symbol", "date", "close"]]


def _last_existing_date(existing: pd.DataFrame):
    if existing.empty or "date" not in existing.columns:
        return None
    dt = pd.to_datetime(existing["date"], errors="coerce").max()
    if pd.isna(dt):
        return None
    return dt.to_pydatetime().replace(tzinfo=UTC)


def main():
    existing = read_existing_leaf().copy()
    existing["date"] = pd.to_datetime(
        existing.get("date", pd.Series([], dtype="datetime64[ns]")),
        errors="coerce",
    )
    existing["close"] = pd.to_numeric(
        existing.get("close", pd.Series([], dtype="float64")),
        errors="coerce",
    )

    last_dt = _last_existing_date(existing)

    # Overlap window prevents "stuck leaf" when EIA publishes late / holes occur.
    if last_dt is None:
        start_date = "2000-01-01"
    else:
        start_date = (last_dt.date() - timedelta(days=OVERLAP_DAYS)).isoformat()

    now_utc = datetime.now(UTC)

    incremental = fetch_incremental(start_date)

    # Combine + dedupe ensures determinism even with overlap pull.
    combined = pd.concat(
        [existing[["symbol", "date", "close"]], incremental],
        ignore_index=True,
    )
    combined["date"] = pd.to_datetime(combined["date"], errors="coerce")
    combined["close"] = pd.to_numeric(combined["close"], errors="coerce")

    combined = (
        combined.dropna(subset=["symbol", "date", "close"])
        .sort_values(["symbol", "date"])
        .drop_duplicates(["symbol", "date"], keep="last")
        .reset_index(drop=True)
    )

    if combined.empty:
        raise ValueError("WTI T1 refresh produced no rows.")

    write_leaf(combined)

    new_last_dt = pd.to_datetime(combined["date"].max()).to_pydatetime().replace(tzinfo=UTC)
    lag_days = (now_utc - new_last_dt).days

    # Governance: fail if still stale after refresh attempt.
    # WTI source publication can legitimately lag across weekends / publication timing,
    # so allow a wider governed tolerance.
    if lag_days > int(WTI_MAX_LAG_DAYS):
        raise ValueError(
            f"WTI T1 freshness failed. last_date={new_last_dt.date()} "
            f"now_utc={now_utc.date()} lag_days={lag_days} allowed={WTI_MAX_LAG_DAYS}"
        )

    print("WTI T1 UPDATE complete.")
    print(f"Total rows: {len(combined)} | Last date: {new_last_dt.date()} | lag_days={lag_days}")


if __name__ == "__main__":
    main()

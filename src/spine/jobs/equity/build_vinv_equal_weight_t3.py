from __future__ import annotations

import io
import os
import sys
from typing import List

import boto3
import pandas as pd

# Inputs & outputs
R2_COMPONENTS_KEY = "spine_us/us_vinv_components.parquet"
R2_VINV_KEY = "spine_us/us_vinv_equal_weight.parquet"

# Output follows equity canonical schema for consistency
CANON_COLS = ["symbol", "date", "open", "high", "low", "close", "volume", "source"]

VINV_SYMBOL = "VINV"
VINV_SOURCE = "vinv_eqw"


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


def _build_equal_weight_index(df_components: pd.DataFrame) -> pd.DataFrame:
    dfx = df_components.copy()
    dfx["symbol"] = dfx["symbol"].astype(str).str.upper()
    dfx["date"] = pd.to_datetime(dfx["date"]).dt.floor("D")

    # Pivot close prices
    px = dfx.pivot_table(index="date", columns="symbol", values="close", aggfunc="last").sort_index()
    px = px.dropna(how="all")

    # Compute daily returns per symbol
    rets = px.pct_change(fill_method=None)

    # Monthly rebalance: equal weight across available names at each month start
    # Implementation: for each date, use equal weights across non-null returns that day
    ew_ret = rets.apply(lambda row: row.dropna().mean() if row.dropna().size > 0 else pd.NA, axis=1)

    ew_ret = ew_ret.dropna()
    level = (1.0 + ew_ret).cumprod() * 100.0

    out = pd.DataFrame(
        {
            "symbol": VINV_SYMBOL,
            "date": level.index,
            "open": level.values,
            "high": level.values,
            "low": level.values,
            "close": level.values,
            "volume": pd.NA,
            "source": VINV_SOURCE,
        }
    )

    out = out[CANON_COLS]
    out["date"] = pd.to_datetime(out["date"]).dt.floor("D")
    return out


def main() -> int:
    print("\n=== VINV EQUAL-WEIGHT BUILD (T3) ===")
    print(f"components_key={R2_COMPONENTS_KEY}")
    print(f"target_key={R2_VINV_KEY}")

    df_components = _read_parquet_from_r2(R2_COMPONENTS_KEY)
    if df_components is None or df_components.empty:
        raise SystemExit("VinV components leaf empty or missing")

    df_vinv = _build_equal_weight_index(df_components)
    if df_vinv.empty:
        raise SystemExit("VinV build produced empty output")

    _upload_parquet_to_r2(df_vinv, R2_VINV_KEY)

    print("VinV equal-weight written.")
    print("rows:", len(df_vinv))
    print("max_date:", df_vinv["date"].max())
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise
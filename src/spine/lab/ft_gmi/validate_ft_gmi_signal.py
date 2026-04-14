from __future__ import annotations

import os
from io import BytesIO
from datetime import datetime, UTC

import boto3
import pandas as pd

R2_FT_GMI_KEY = os.getenv("R2_FT_GMI_KEY", "lab/ft_gmi/ft_gmi_daily.parquet")
R2_BUCKET = os.getenv("R2_BUCKET") or os.getenv("R2_BUCKET_NAME")
R2_ENDPOINT = os.getenv("R2_ENDPOINT")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_REGION = os.getenv("R2_REGION")

MAX_ALLOWED_LAG_DAYS = int(os.getenv("FT_GMI_MAX_LAG_DAYS", "10"))
MAX_ALLOWED_DOD_CHANGE = float(os.getenv("FT_GMI_MAX_DOD_CHANGE", "25"))
MAX_ALLOWED_COMPONENT_DISPERSION = float(os.getenv("FT_GMI_MAX_COMPONENT_DISPERSION", "60"))

REQUIRED_COLUMNS = [
    "date",
    "ft_gmi_score",
    "regime_bucket",
    "rates_stress",
    "fx_stress",
    "energy_stress",
    "equity_stress",
    "credit_stress",
    "model_version",
    "build_ts_utc",
]

OPTIONAL_COLUMNS = [
    "rates_stress_fwd45d",
    "fx_stress_fwd45d",
    "energy_stress_fwd45d",
    "equity_stress_fwd45d",
    "credit_stress_fwd45d",
    "ft_gmi_score_fwd45d",
    "rates_weight",
    "fx_weight",
    "energy_weight",
    "equity_weight",
    "credit_weight",
    "top_driver",
    "top_driver_score",
    "validator_status",
    "validator_warning_count",
    "max_component",
    "min_component",
    "dispersion_score",
]

VALID_REGIME_BUCKETS = {"Stable", "Tension", "Crisis Risk", "Contagion"}


def _s3():
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name=R2_REGION,
    )


def _read_leaf() -> pd.DataFrame:
    if not R2_BUCKET:
        raise ValueError("R2 bucket not set")
    obj = _s3().get_object(Bucket=R2_BUCKET, Key=R2_FT_GMI_KEY)
    return pd.read_parquet(BytesIO(obj["Body"].read()))


def main() -> None:
    df = _read_leaf().copy()

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["build_ts_utc"] = pd.to_datetime(df["build_ts_utc"], errors="coerce")

    numeric_cols = [
        "ft_gmi_score",
        "rates_stress",
        "fx_stress",
        "energy_stress",
        "equity_stress",
        "credit_stress",
    ] + [c for c in OPTIONAL_COLUMNS if c in df.columns and ("score" in c or "stress" in c or "weight" in c or "count" in c)]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    if df.duplicated(subset=["date"]).any():
        raise ValueError("Duplicate FT-GMI dates found")

    if not df["date"].is_monotonic_increasing:
        df = df.sort_values("date").reset_index(drop=True)

    nulls = df[REQUIRED_COLUMNS].isna().sum()
    bad_nulls = nulls[nulls > 0]
    if not bad_nulls.empty:
        raise ValueError(f"Nulls in required columns: {bad_nulls.to_dict()}")

    score_cols = [c for c in df.columns if c.endswith("_stress") or c.endswith("_score") or c.endswith("_fwd45d")]
    for col in score_cols:
        bad = df[(df[col] < 0) | (df[col] > 100)]
        if not bad.empty:
            raise ValueError(f"{col} outside 0-100 range")

    bad_bucket = df[~df["regime_bucket"].isin(VALID_REGIME_BUCKETS)]
    if not bad_bucket.empty:
        raise ValueError(f"Invalid regime_bucket values: {sorted(set(bad_bucket['regime_bucket']))}")

    latest = df["date"].max().to_pydatetime().replace(tzinfo=UTC)
    now_utc = datetime.now(UTC)
    lag_days = (now_utc - latest).days
    if lag_days > MAX_ALLOWED_LAG_DAYS:
        raise ValueError(
            f"FT-GMI freshness failed. latest={latest.date()} now_utc={now_utc.date()} "
            f"lag_days={lag_days} allowed={MAX_ALLOWED_LAG_DAYS}"
        )

    if len(df) > 1:
        dod = df["ft_gmi_score"].diff().abs().dropna()
        if not dod.empty and float(dod.max()) > MAX_ALLOWED_DOD_CHANGE:
            print(f"[FT-GMI][VALIDATE][WARN] max_dod_change={float(dod.max()):.2f} > {MAX_ALLOWED_DOD_CHANGE:.2f}")

    component_cols = ["rates_stress", "fx_stress", "energy_stress", "equity_stress", "credit_stress"]
    dispersion = df[component_cols].max(axis=1) - df[component_cols].min(axis=1)
    max_disp = float(dispersion.max())
    if max_disp > MAX_ALLOWED_COMPONENT_DISPERSION:
        print(f"[FT-GMI][VALIDATE][WARN] max_component_dispersion={max_disp:.2f} > {MAX_ALLOWED_COMPONENT_DISPERSION:.2f}")

    weight_cols = [c for c in ["rates_weight", "fx_weight", "energy_weight", "equity_weight", "credit_weight"] if c in df.columns]
    if weight_cols:
        latest_row = df.iloc[-1]
        weight_sum = float(sum(float(latest_row[c]) for c in weight_cols))
        if abs(weight_sum - 1.0) > 1e-6:
            raise ValueError(f"Weight sum != 1.0 on latest row: {weight_sum}")

    print(
        f"[FT-GMI][VALIDATE] OK "
        f"(rows={len(df):,}, latest={latest.date()}, "
        f"min={df['ft_gmi_score'].min():.2f}, max={df['ft_gmi_score'].max():.2f})"
    )


if __name__ == "__main__":
    main()


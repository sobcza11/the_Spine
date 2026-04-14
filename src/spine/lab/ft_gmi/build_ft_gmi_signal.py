from __future__ import annotations

import os
from io import BytesIO
from datetime import datetime, UTC

import boto3
import pandas as pd


R2_BUCKET = os.getenv("R2_BUCKET") or os.getenv("R2_BUCKET_NAME")
R2_ENDPOINT = os.getenv("R2_ENDPOINT")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_REGION = os.getenv("R2_REGION")

R2_RATES_KEY = os.getenv("R2_RATES_KEY", "spine_us/us_ir_diff_canonical.parquet")
R2_FX_KEY = os.getenv("R2_FX_KEY", "spine_us/us_fx_spot_canonical_t2.parquet")
R2_ENERGY_KEY = os.getenv("R2_ENERGY_KEY", "spine_us/leaves/energy/wti_price_t1.parquet")
R2_EQUITY_KEY = os.getenv("R2_EQUITY_KEY", "spine_us/us_vinv_components.parquet")

R2_FT_GMI_KEY = os.getenv("R2_FT_GMI_KEY", "lab/ft_gmi/ft_gmi_daily.parquet")

MODEL_VERSION = "ft_gmi_v2_opt_lag"


def _s3():
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name=R2_REGION,
    )


def _read_parquet(key: str) -> pd.DataFrame:
    if not R2_BUCKET:
        raise ValueError("R2_BUCKET or R2_BUCKET_NAME not set")
    obj = _s3().get_object(Bucket=R2_BUCKET, Key=key)
    return pd.read_parquet(BytesIO(obj["Body"].read()))


def _write_parquet(df: pd.DataFrame, key: str) -> None:
    if not R2_BUCKET:
        raise ValueError("R2_BUCKET or R2_BUCKET_NAME not set")

    buf = BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    _s3().put_object(Bucket=R2_BUCKET, Key=key, Body=buf.getvalue())


def _to_score_from_rank(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    if s.notna().sum() == 0:
        return pd.Series([0.0] * len(series), index=series.index)
    return (s.rank(pct=True) * 100.0).fillna(0.0)


def _pick_value_col(df: pd.DataFrame, candidates: list[str], label: str) -> str:
    for c in candidates:
        if c in df.columns:
            return c
    raise ValueError(f"{label} leaf missing expected value columns: {candidates}")


def _build_rates_stress() -> pd.DataFrame:
    df = _read_parquet(R2_RATES_KEY).copy()

    date_col = "as_of_date"
    value_col = "diff_10y_bp"

    if date_col not in df.columns:
        raise ValueError(f"Rates leaf missing {date_col}. Found: {list(df.columns)}")
    if value_col not in df.columns:
        raise ValueError(f"Rates leaf missing {value_col}. Found: {list(df.columns)}")

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.rename(columns={date_col: "date", value_col: "rates_raw"})

    if "pair" in df.columns:
        use = df[df["pair"].astype(str).str.upper().eq("USD_EUR")].copy()
        if use.empty:
            use = df.copy()
    else:
        use = df.copy()

    daily = use.groupby("date", as_index=False)["rates_raw"].mean()
    daily["rates_stress"] = _to_score_from_rank(daily["rates_raw"].abs())
    return daily[["date", "rates_stress"]]


def _build_fx_stress() -> pd.DataFrame:
    df = _read_parquet(R2_FX_KEY).copy()

    date_col = _pick_date_col(df, "FX")
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.rename(columns={date_col: "date"})

    value_col = _pick_value_col(df, ["close", "value", "rate"], "FX")

    if "symbol" in df.columns:
        use = df[df["symbol"].astype(str).str.upper().isin(["DXY", "EURUSD", "USDJPY"])].copy()
        if use.empty:
            use = df.copy()
    else:
        use = df.copy()

    daily = use.groupby("date", as_index=False)[value_col].mean()
    daily["fx_stress"] = _to_score_from_rank(daily[value_col].pct_change().abs())
    return daily[["date", "fx_stress"]]


def _build_energy_stress() -> pd.DataFrame:
    df = _read_parquet(R2_ENERGY_KEY).copy()

    date_col = _pick_date_col(df, "Energy")
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.rename(columns={date_col: "date"})

    value_col = _pick_value_col(df, ["close", "value", "price"], "Energy")

    daily = df.groupby("date", as_index=False)[value_col].mean()
    daily["energy_stress"] = _to_score_from_rank(daily[value_col].pct_change().abs())
    return daily[["date", "energy_stress"]]


def _build_equity_stress() -> pd.DataFrame:
    df = _read_parquet(R2_EQUITY_KEY).copy()

    date_col = _pick_date_col(df, "Equity")
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.rename(columns={date_col: "date"})

    value_col = _pick_value_col(df, ["ret_m", "return", "value", "close"], "Equity")

    daily = df.groupby("date", as_index=False)[value_col].mean()
    daily["equity_stress"] = _to_score_from_rank(daily[value_col].abs())
    return daily[["date", "equity_stress"]]


def _build_credit_proxy(rates_df: pd.DataFrame, fx_df: pd.DataFrame, equity_df: pd.DataFrame) -> pd.DataFrame:
    df = rates_df.merge(fx_df, on="date", how="outer").merge(equity_df, on="date", how="outer")
    df = df.sort_values("date").reset_index(drop=True)

    for c in ["rates_stress", "fx_stress", "equity_stress"]:
        if c not in df.columns:
            df[c] = 0.0

    df["credit_stress"] = (
        0.50 * df["rates_stress"].fillna(0.0)
        + 0.25 * df["fx_stress"].fillna(0.0)
        + 0.25 * df["equity_stress"].fillna(0.0)
    )
    return df[["date", "credit_stress"]]


def _regime_bucket(score: float) -> str:
    if score <= 30:
        return "Stable"
    if score <= 55:
        return "Tension"
    if score <= 75:
        return "Crisis Risk"
    return "Contagion"


def _require_columns(df: pd.DataFrame, cols: list[str], label: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{label} missing required columns: {missing}")


def main() -> None:
    rates = _build_rates_stress()
    fx = _build_fx_stress()
    energy = _build_energy_stress()
    equity = _build_equity_stress()
    credit = _build_credit_proxy(rates, fx, equity)

    df = (
        rates.merge(fx, on="date", how="outer")
        .merge(energy, on="date", how="outer")
        .merge(equity, on="date", how="outer")
        .merge(credit, on="date", how="outer")
        .sort_values("date")
        .reset_index(drop=True)
    )

    component_cols = [
        "rates_stress",
        "fx_stress",
        "energy_stress",
        "equity_stress",
        "credit_stress",
    ]

    for c in component_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").ffill().fillna(0.0)

    df["rates_weight"] = 0.30
    df["fx_weight"] = 0.20
    df["energy_weight"] = 0.15
    df["equity_weight"] = 0.25
    df["credit_weight"] = 0.10

    df["ft_gmi_score"] = (
        df["rates_stress"] * df["rates_weight"]
        + df["fx_stress"] * df["fx_weight"]
        + df["energy_stress"] * df["energy_weight"]
        + df["equity_stress"] * df["equity_weight"]
        + df["credit_stress"] * df["credit_weight"]
    ).clip(0, 100)

    df["max_component"] = df[component_cols].max(axis=1)
    df["min_component"] = df[component_cols].min(axis=1)
    df["dispersion_score"] = df["max_component"] - df["min_component"]
    df["top_driver"] = df[component_cols].idxmax(axis=1)
    df["top_driver_score"] = df[component_cols].max(axis=1)

    # Optional fields expected from downstream / feature engineering conventions.
    # Keep build deterministic & flexible: populate only if not already present.
    optional_defaults = {
        "validator_status": "OK",
        "validator_warning_count": 0,
    }
    for col, default in optional_defaults.items():
        if col not in df.columns:
            df[col] = default

    df["regime_bucket"] = df["ft_gmi_score"].apply(_regime_bucket)
    df["model_version"] = MODEL_VERSION
    df["build_ts_utc"] = datetime.now(UTC)

    # If optimal-lag / forward-impact columns are later added upstream, preserve them.
    optional_cols = [
        "rates_stress_opt_lag_days",
        "fx_stress_opt_lag_days",
        "energy_stress_opt_lag_days",
        "equity_stress_opt_lag_days",
        "credit_stress_opt_lag_days",
        "rates_stress_opt_lag_corr",
        "fx_stress_opt_lag_corr",
        "energy_stress_opt_lag_corr",
        "equity_stress_opt_lag_corr",
        "credit_stress_opt_lag_corr",
        "rates_stress_fwd_opt",
        "fx_stress_fwd_opt",
        "energy_stress_fwd_opt",
        "equity_stress_fwd_opt",
        "credit_stress_fwd_opt",
        "ft_gmi_score_fwd_opt",
    ]

    base_cols = [
        "date",
        "ft_gmi_score",
        "regime_bucket",
        "rates_stress",
        "fx_stress",
        "energy_stress",
        "equity_stress",
        "credit_stress",
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
        "model_version",
        "build_ts_utc",
    ]

    keep_cols = base_cols + [c for c in optional_cols if c in df.columns]

    _require_columns(df, base_cols, "FT-GMI build output")

    out = (
        df[keep_cols]
        .dropna(subset=["date"])
        .sort_values("date")
        .drop_duplicates(subset=["date"], keep="last")
        .reset_index(drop=True)
    )

    if out.empty:
        raise ValueError("FT-GMI build produced zero rows.")

    print(f"[FT-GMI][WRITE] bucket={R2_BUCKET} key={R2_FT_GMI_KEY} rows={len(out)}")
    _write_parquet(out, R2_FT_GMI_KEY)
    print("[FT-GMI][WRITE] complete")
    print(
        f"[FT-GMI] Built ft_gmi_daily.parquet "
        f"(rows={len(out):,}, latest={pd.to_datetime(out['date']).max().date()}, "
        f"min_score={out['ft_gmi_score'].min():.2f}, max_score={out['ft_gmi_score'].max():.2f})"
    )

def _pick_date_col(df: pd.DataFrame, label: str) -> str:
    candidates = ["date", "as_of_date", "Date", "period", "datetime", "timestamp"]
    for c in candidates:
        if c in df.columns:
            return c
    raise ValueError(f"{label} leaf missing expected date column. Found: {list(df.columns)}")

if __name__ == "__main__":
    main()




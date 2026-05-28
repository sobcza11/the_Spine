from __future__ import annotations

from pathlib import Path
import pandas as pd


REPO_ROOT = Path.cwd()

CB_PATH = REPO_ROOT / "data" / "llm" / "approved_inputs" / "isovector_macro_cb_view.parquet"
RATES_PATH = REPO_ROOT / "data" / "llm" / "approved_inputs" / "isovector_macro_cb_rates_join.parquet"
WTI_PATH = REPO_ROOT / "data" / "wti" / "wti_inflation_pressure.parquet"
FX_PATH = REPO_ROOT / "data" / "spine_us" / "us_fx_spot_cross_t2.parquet"

OUTPUT_PATH = REPO_ROOT / "data" / "serving" / "fx" / "fx_zt_v1.parquet"


def normalize(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    if s.max() == s.min():
        return pd.Series(0, index=s.index)
    return 2 * ((s - s.min()) / (s.max() - s.min())) - 1


def load_cb():
    df = pd.read_parquet(CB_PATH)
    df["date"] = pd.to_datetime(df["date"])
    df["cb_divergence"] = df["policy_tone"] - df["uncertainty"]
    return df[["date", "cb_divergence"]]


def load_rates():
    df = pd.read_parquet(RATES_PATH)
    df["date"] = pd.to_datetime(df["date"])
    df["rates_alignment"] = pd.to_numeric(df["it_de_10y_spread_z"], errors="coerce")
    return df[["date", "rates_alignment"]]


def load_fx():
    df = pd.read_parquet(FX_PATH).copy()
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    df = df.sort_values(["symbol", "date"])
    df["fx_ret"] = df.groupby("symbol")["close"].pct_change()

    df["date"] = df["date"].dt.to_period("M").dt.to_timestamp()

    monthly = (
        df.groupby("date", as_index=False)["fx_ret"]
        .mean()
        .rename(columns={"fx_ret": "fx_pressure"})
    )

    return monthly[["date", "fx_pressure"]]


def load_wti():
    df = pd.read_parquet(WTI_PATH).copy()
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
    df["wti_norm_raw"] = pd.to_numeric(df["inflation_pressure_z"], errors="coerce")
    df["date"] = df["date"].dt.to_period("M").dt.to_timestamp()

    monthly = (
        df.groupby("date", as_index=False)["wti_norm_raw"]
        .mean()
        .rename(columns={"wti_norm_raw": "wti_return"})
    )

    return monthly[["date", "wti_return"]]


def main():
    cb = load_cb()
    rates = load_rates()
    fx = load_fx()
    wti = load_wti()

    df = cb.merge(rates, on="date", how="outer")
    df = df.merge(fx, on="date", how="outer")
    df = df.merge(wti, on="date", how="outer")

    df = df.sort_values("date")
    df = df.groupby("date", as_index=False).mean()
    df = df.sort_values("date").reset_index(drop=True)

    df["cb_norm"] = normalize(df["cb_divergence"])
    df["rates_norm"] = normalize(df["rates_alignment"])
    df["fx_norm"] = normalize(df["fx_pressure"])
    df["wti_norm"] = normalize(df["wti_return"])

    df["fx_zt"] = (
        0.4 * df["cb_norm"].fillna(0)
        + 0.3 * df["rates_norm"].fillna(0)
        + 0.2 * df["fx_norm"].fillna(0)
        + 0.1 * df["wti_norm"].fillna(0)
    )

    out = df[[
        "date",
        "cb_norm",
        "rates_norm",
        "fx_norm",
        "wti_norm",
        "fx_zt"
    ]]

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUTPUT_PATH, index=False)

    print(f"FX Z_t written: {OUTPUT_PATH}")
    print(out.tail())


if __name__ == "__main__":
    main()
    
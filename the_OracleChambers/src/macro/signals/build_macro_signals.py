from pathlib import Path
import pandas as pd
import numpy as np


# Root of the_Spine repo (…\the_Spine)
ROOT = Path(__file__).resolve().parents[2]

# Input: output from fred_ingest.py
IN_PATH = ROOT / "data" / "macro" / "fred_raw_monthly.parquet"

# Output: macro signal panel for the_Spine
OUT_PATH = ROOT / "data" / "macro" / "macro_signals_monthly.parquet"


def main() -> None:
    print(f"[MacroSignals] Loading raw FRED panel: {IN_PATH}")

    df = pd.read_parquet(IN_PATH)
    df["date"] = pd.to_datetime(df["date"])

    # Wide panel: one column per macro series
    df_wide = (
        df.pivot(index="date", columns="series_name", values="value")
        .sort_index()
    )

    # Normalize to numeric and float64, handle pandas.NA safely
    df_wide = df_wide.apply(pd.to_numeric, errors="coerce").astype("float64")
    df_wide.columns.name = None

    # ---------- Core signals ----------

    # 1) Yield curve spreads
    if "us_10y_yield" in df_wide.columns and "us_2y_yield" in df_wide.columns:
        df_wide["curve_10y_2y"] = df_wide["us_10y_yield"] - df_wide["us_2y_yield"]

    if "us_10y_yield" in df_wide.columns and "us_3m_yield" in df_wide.columns:
        df_wide["curve_10y_3m"] = df_wide["us_10y_yield"] - df_wide["us_3m_yield"]

    # 2) Inflation momentum: 3-month annualized % change in CPI & PCE
    def calc_3m_annualized(series: pd.Series) -> pd.Series:
        s = pd.to_numeric(series, errors="coerce")
        return s.pct_change(3) * 4.0

    for price_col, out_col in [
        ("cpi_all_items", "cpi_infl_3m_annualized"),
        ("pce_deflator", "pce_infl_3m_annualized"),
    ]:
        if price_col in df_wide.columns:
            df_wide[out_col] = calc_3m_annualized(df_wide[price_col])

    # 3) Energy impulse (WTI 3m & 12m % changes)
    if "wti_spot_price" in df_wide.columns:
        wti = pd.to_numeric(df_wide["wti_spot_price"], errors="coerce")
        df_wide["wti_3m_change"] = wti.pct_change(3)
        df_wide["wti_12m_change"] = wti.pct_change(12)

    # 4) Policy stance: Fed Funds – trailing CPI inflation (12m)
    if "fed_funds_rate" in df_wide.columns and "cpi_all_items" in df_wide.columns:
        cpi = pd.to_numeric(df_wide["cpi_all_items"], errors="coerce")
        cpi_12m = cpi.pct_change(12) * 100.0  # % y/y
        df_wide["policy_stance_realish"] = df_wide["fed_funds_rate"] - cpi_12m

    # ---------- Finalize & save ----------

    df_out = df_wide.reset_index().sort_values("date")
    print("[MacroSignals] Output shape:", df_out.shape)
    print(df_out.head())

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_parquet(OUT_PATH, index=False)
    print(f"[MacroSignals] Wrote {OUT_PATH}")


if __name__ == "__main__":
    main()

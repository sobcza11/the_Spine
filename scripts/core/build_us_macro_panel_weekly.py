#!/usr/bin/env python
import sys
import os
import logging
import pandas as pd

log = logging.getLogger("MACRO-PANEL")


from utils.storage_r2 import upload_file_to_r2  # reuse your R2 helper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
log = logging.getLogger("SCRIPT-MACRO-PANEL")

COT_FACTORS_PATH = "data/spine_us/us_cftc_cot_factors_canonical.parquet"
FX_RETURNS_PATH  = "data/spine_us/us_fx_returns_canonical.parquet"  # placeholder
IR_YIELDS_PATH   = "data/us/yieldcurve_10y3m/us_yieldcurve_10y3m_canonical.parquet"  # placeholder
EQ_INDEX_PATH    = "data/spine_us/us_eq_index_canonical.parquet"  # placeholder

MACRO_PANEL_PATH = "data/canonical/us_macro_panel_weekly_canonical.parquet"
MACRO_PANEL_R2_KEY = "spine_us/us_macro_panel_weekly_canonical.parquet"


def load_cot_factors() -> pd.DataFrame:
    if not os.path.exists(COT_FACTORS_PATH):
        raise FileNotFoundError(f"COT factors not found at {COT_FACTORS_PATH}")
    df = pd.read_parquet(COT_FACTORS_PATH)
    df["as_of_date"] = pd.to_datetime(df["as_of_date"])
    return df


def load_fx_returns() -> pd.DataFrame:
    if not os.path.exists(FX_RETURNS_PATH):
        log.warning(
            "[MACRO-PANEL] FX returns file not found at %s – building panel without FX.",
            FX_RETURNS_PATH,
        )
        return pd.DataFrame()
    df = pd.read_parquet(FX_RETURNS_PATH)
    df["as_of_date"] = pd.to_datetime(df["as_of_date"])
    return df

def load_ir_yields() -> pd.DataFrame:
    if not os.path.exists(IR_YIELDS_PATH):
        log.warning(
            "[MACRO-PANEL] IR yields file not found at %s – building panel without IR.",
            IR_YIELDS_PATH,
        )
        return pd.DataFrame()
    df = pd.read_parquet(IR_YIELDS_PATH)
    df["as_of_date"] = pd.to_datetime(df["as_of_date"])
    return df

def load_ir_features() -> pd.DataFrame:
    """
    Expectation: IR leaf has:
        as_of_date, symbol, yield
    We pivot: ust2y_yield, ust10y_yield, and compute 10y-2y spread.
    """
    df = pd.read_parquet(IR_YIELDS_PATH)
    df["as_of_date"] = pd.to_datetime(df["as_of_date"])

    pivot = df.pivot(index="as_of_date", columns="symbol", values="yield")

    # rename columns if they come out like 'UST_2Y', 'UST_10Y'
    if "UST_2Y" in pivot.columns and "UST_10Y" in pivot.columns:
        pivot["yc_10y_2y_spread"] = pivot["UST_10Y"] - pivot["UST_2Y"]

    pivot = pivot.reset_index()
    return pivot

def load_eq_index() -> pd.DataFrame:
    if not os.path.exists(EQ_INDEX_PATH):
        log.warning(
            "[MACRO-PANEL] Equity index file not found at %s – building panel without EQ.",
            EQ_INDEX_PATH,
        )
        return pd.DataFrame()
    df = pd.read_parquet(EQ_INDEX_PATH)
    df["as_of_date"] = pd.to_datetime(df["as_of_date"])
    return df


def load_pmi_headline_weekly() -> pd.DataFrame:
    """
    Takes monthly PMI headline and forward-fills to weekly dates.

    Expectation:
        as_of_date (month end or release date)
        ism_manu
        ism_serv
    Adjust col names to your actual PMI leaf.
    """
    df = pd.read_parquet(PMI_HEADLINE_PATH)
    df["as_of_date"] = pd.to_datetime(df["as_of_date"])
    df = df.sort_values("as_of_date")

    # For now, we'll simply keep it at monthly frequency; when merged onto
    # weekly COT dates, pandas will broadcast via merge_asof or left join.
    return df


def build_macro_panel() -> pd.DataFrame:
    cot = load_cot_factors()
    fx  = load_fx_returns()
    ir  = load_ir_yields()
    eq  = load_eq_index()

    panel = cot.copy()   # weekly index + COT factors is your core

    if not fx.empty:
        panel = panel.merge(fx, on="as_of_date", how="left", suffixes=("", "_fx"))
    else:
        log.info("[MACRO-PANEL] FX empty – skipping FX merge.")

    if not ir.empty:
        panel = panel.merge(ir, on="as_of_date", how="left", suffixes=("", "_ir"))
    else:
        log.info("[MACRO-PANEL] IR empty – skipping IR merge.")

    if not eq.empty:
        panel = panel.merge(eq, on="as_of_date", how="left", suffixes=("", "_eq"))
    else:
        log.info("[MACRO-PANEL] EQ empty – skipping EQ merge.")

    return panel


def main(argv=None) -> int:
    log.info("[MACRO-PANEL] Building US macro weekly panel …")
    df_panel = build_macro_panel()

    os.makedirs(os.path.dirname(MACRO_PANEL_PATH), exist_ok=True)
    df_panel.to_parquet(MACRO_PANEL_PATH, index=False)
    log.info("[MACRO-PANEL] Wrote panel to %s (rows=%d)", MACRO_PANEL_PATH, len(df_panel))

    upload_file_to_r2(MACRO_PANEL_PATH, MACRO_PANEL_R2_KEY)
    log.info("[MACRO-PANEL] Uploaded panel to R2 key=%s", MACRO_PANEL_R2_KEY)

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))


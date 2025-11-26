import os
import logging

import pandas as pd
import matplotlib.pyplot as plt

log = logging.getLogger("COT-SANITY")

COT_FACTORS_PATH = "data/spine_us/us_cftc_cot_factors_canonical.parquet"
FX_RETURNS_PATH  = "data/spine_us/us_fx_returns_canonical.parquet"
IR_YIELDS_PATH   = "data/us/yieldcurve_10y3m/us_yieldcurve_10y3m_canonical.parquet"
EQ_INDEX_PATH    = "data/spine_us/us_eq_index_canonical.parquet"

def _load_fx_returns_or_none() -> pd.DataFrame | None:
    """Load FX returns if available, otherwise return None."""
    if not os.path.exists(FX_RETURNS_PATH):
        log.warning(
            "FX returns file not found at %s – skipping FX-based sanity plots.",
            FX_RETURNS_PATH,
        )
        return None
    df = pd.read_parquet(FX_RETURNS_PATH)
    if "as_of_date" in df.columns:
        df["as_of_date"] = pd.to_datetime(df["as_of_date"])
    return df


def _load_ir_yields_or_none() -> pd.DataFrame | None:
    """Load 10Y yield data if available, otherwise return None."""
    if not os.path.exists(IR_YIELDS_PATH):
        log.warning(
            "10Y yields file not found at %s – skipping duration/yields sanity plot.",
            IR_YIELDS_PATH,
        )
        return None
    df = pd.read_parquet(IR_YIELDS_PATH)
    if "as_of_date" in df.columns:
        df["as_of_date"] = pd.to_datetime(df["as_of_date"])
    return df

def _load_eq_index_or_none() -> pd.DataFrame | None:
    """Load SPX / equity index data if available, otherwise return None."""
    if not os.path.exists(EQ_INDEX_PATH):
        log.warning(
            "Equity index file not found at %s – skipping risk-on/SPX sanity plot.",
            EQ_INDEX_PATH,
        )
        return None
    df = pd.read_parquet(EQ_INDEX_PATH)
    if "as_of_date" in df.columns:
        df["as_of_date"] = pd.to_datetime(df["as_of_date"])
    return df


def _load_fx_returns_or_none() -> pd.DataFrame | None:
    if not os.path.exists(FX_RETURNS_PATH):
        log.warning(
            "FX returns file not found at %s – skipping FX-based sanity plots.",
            FX_RETURNS_PATH,
        )
        return None
    return pd.read_parquet(FX_RETURNS_PATH)

def plot_usd_spec_vs_eurusd():
    log.info("Plot 1/3: USD Crowding vs EURUSD")

    cot = pd.read_parquet(COT_FACTORS_PATH)
    fx = _load_fx_returns_or_none()
    if fx is None:
        log.warning(
            "Skipping Plot 1 (USD spec vs EURUSD) because FX returns are missing."
        )
        return
    
    cot = pd.read_parquet(COT_FACTORS_PATH)
    fx = pd.read_parquet(FX_RETURNS_PATH)

    # assume fx has: as_of_date, pair, ret_1w
    eur = fx[fx["pair"] == "EURUSD"][["as_of_date", "ret_1w"]].copy()
    eur.rename(columns={"ret_1w": "eurusd_ret_1w"}, inplace=True)

    df = cot.merge(eur, on="as_of_date", how="inner")

    fig, ax1 = plt.subplots()
    ax1.plot(df["as_of_date"], df["usd_spec_crowding_z"], label="USD Spec Crowding (z)")
    ax1.set_ylabel("USD Spec Crowding (z)")

    ax2 = ax1.twinx()
    ax2.plot(df["as_of_date"], df["eurusd_ret_1w"], linestyle="--", label="EURUSD 1w Return")
    ax2.set_ylabel("EURUSD 1w Return")

    fig.suptitle("USD Spec Crowding vs EURUSD Weekly Returns")
    plt.show()


def plot_duration_crowding_vs_10y():
    log.info("Plot 2/3: Duration Crowding vs 10Y Yield")

    cot = pd.read_parquet(COT_FACTORS_PATH)
    ir = _load_ir_yields_or_none()
    if ir is None:
        log.warning(
            "Skipping Plot 2 (duration vs 10Y) because IR yields are missing."
        )
        return

    # existing logic below: join COT duration factor with 10Y yield & plot
    # e.g.
    # cot_sub = cot[...]
    # ir_sub  = ir[...]
    # merged  = ...
    # merged.plot(...)



def plot_risk_on_vs_spx():
    log.info("Plot 3/3: Risk-On vs SPX")

    cot = pd.read_parquet(COT_FACTORS_PATH)
    eq = _load_eq_index_or_none()
    if eq is None:
        log.warning(
            "Skipping Plot 3 (risk-on vs SPX) because equity index data is missing."
        )
        return
    
    cot = pd.read_parquet(COT_FACTORS_PATH)
    eq = pd.read_parquet(EQ_INDEX_PATH)

    # assume eq has: as_of_date, symbol, ret_1w
    spx = eq[eq["symbol"] == "SPX"][["as_of_date", "ret_1w"]].copy()
    spx.rename(columns={"ret_1w": "spx_ret_1w"}, inplace=True)

    df = cot.merge(spx, on="as_of_date", how="inner")

    plt.scatter(df["risk_on_cftc_factor"], df["spx_ret_1w"], alpha=0.4)
    plt.axvline(0, linewidth=1)
    plt.axhline(0, linewidth=1)
    plt.xlabel("Risk-On CFTC Factor")
    plt.ylabel("SPX 1w Return")
    plt.title("Risk-On Positioning vs SPX Weekly Returns")
    plt.show()


def main():
    log.info("Plot 1/3: USD Crowding vs EURUSD")
    plot_usd_spec_vs_eurusd()

    log.info("Plot 2/3: Duration Crowding vs 10Y Yield")
    plot_duration_crowding_vs_10y()

    log.info("Plot 3/3: Risk-On Factor vs SPX Returns")
    plot_risk_on_vs_spx()


if __name__ == "__main__":
    main()


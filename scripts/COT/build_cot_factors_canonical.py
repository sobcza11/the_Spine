#!/usr/bin/env python
"""
Build COT factor leaf from canonical TFF COT data for the_Spine.

Input:
    data/spine_us/us_cftc_cot_tff_canonical.parquet

Output:
    data/spine_us/us_cftc_cot_factors_canonical.parquet
    R2: spine_us/us_cftc_cot_factors_canonical.parquet

Grain:
    one row per as_of_date

Columns:
    as_of_date
    usd_spec_crowding_z
    fx_spec_momo_z
    ust_curve_steepen_z
    ust_duration_crowding_z
    eq_index_crowding_z
    risk_on_cftc_factor
    global_spec_pressure_z
    dealer_hedge_stress_rates
    dealer_hedge_stress_fx
    cross_asset_spec_momo_z
    source_report_type
    created_at_utc
    updated_at_utc
"""

import os
import sys
import logging
from dataclasses import dataclass
from typing import List

import numpy as np
import pandas as pd

from utils.storage_r2 import upload_file_to_r2  # requires PYTHONPATH=src

log = logging.getLogger("SCRIPT-COT-FACTORS")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------


@dataclass
class CotFactorConfig:
    input_cot_parquet: str = "data/spine_us/us_cftc_cot_tff_canonical.parquet"
    output_local_path: str = "data/spine_us/us_cftc_cot_factors_canonical.parquet"
    r2_key: str = "spine_us/us_cftc_cot_factors_canonical.parquet"


# Spine symbol sets (adjust if your universe mapping uses different IDs)
FX_G7 = [
    "FX_EUR",
    "FX_JPY",
    "FX_GBP",
    "FX_CHF",
    "FX_AUD",
    "FX_CAD",
    "FX_NZD",
]

FX_AUD = "FX_AUD"
FX_CAD = "FX_CAD"
FX_JPY = "FX_JPY"

UST_SHORT = ["UST_2Y"]
UST_LONG = ["UST_10Y", "UST_30Y"]
UST_10Y = "UST_10Y"

EQ_INDEX = ["EQ_SPX", "EQ_NDX"]  # adjust if needed


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _zscore_series(s: pd.Series) -> pd.Series:
    """Simple cross-time z-score for a time series."""
    m = s.mean()
    std = s.std(ddof=0)
    if std == 0 or np.isnan(std):
        return pd.Series(index=s.index, data=np.nan)
    return (s - m) / std


def _mean_net_z(
    df: pd.DataFrame,
    symbols: List[str],
    trader_group: str = "LEVERAGED_FUNDS",
) -> pd.Series:
    """
    Mean of net_contracts_zscore_52w across a set of symbols for a given group.
    Returns a Series indexed by as_of_date.
    """
    sub = df[
        (df["trader_group"] == trader_group)
        & (df["spine_symbol"].isin(symbols))
    ].copy()
    if sub.empty:
        return pd.Series(dtype=float)

    grouped = sub.groupby("as_of_date")["net_contracts_zscore_52w"].mean()
    return grouped.sort_index()


def _symbol_net_z(
    df: pd.DataFrame,
    symbol: str,
    trader_group: str = "LEVERAGED_FUNDS",
) -> pd.Series:
    """
    net_contracts_zscore_52w series for a single symbol & trader group.
    """
    sub = df[
        (df["trader_group"] == trader_group)
        & (df["spine_symbol"] == symbol)
    ]
    if sub.empty:
        return pd.Series(dtype=float)
    s = sub.set_index("as_of_date")["net_contracts_zscore_52w"].sort_index()
    return s


def _mean_delta_net_1w(
    df: pd.DataFrame,
    symbols: List[str],
    trader_group: str = "LEVERAGED_FUNDS",
) -> pd.Series:
    """
    Mean 1w delta of net contracts across a group of symbols.
    """
    sub = df[
        (df["trader_group"] == trader_group)
        & (df["spine_symbol"].isin(symbols))
    ]
    if sub.empty:
        return pd.Series(dtype=float)
    grouped = sub.groupby("as_of_date")["delta_net_contracts_1w"].mean()
    return grouped.sort_index()


# ---------------------------------------------------------------------
# Factor construction
# ---------------------------------------------------------------------


def build_cot_factors(cfg: CotFactorConfig) -> pd.DataFrame:
    log.info(
        "[COT-FACTORS] Loading canonical TFF COT leaf: %s", cfg.input_cot_parquet
    )
    if not os.path.exists(cfg.input_cot_parquet):
        raise FileNotFoundError(
            f"Canonical COT file not found at {cfg.input_cot_parquet}. "
            "Run scripts/build_cot_tff_canonical.py first."
        )

    df = pd.read_parquet(cfg.input_cot_parquet)

    if "net_contracts_zscore_52w" not in df.columns:
        raise ValueError("Input COT leaf missing 'net_contracts_zscore_52w'.")
    if "delta_net_contracts_1w" not in df.columns:
        raise ValueError("Input COT leaf missing 'delta_net_contracts_1w'.")

    df["as_of_date"] = pd.to_datetime(df["as_of_date"])

    # -----------------------------------------------------------------
    # FACTOR 1: USD Spec Crowding (Leveraged Funds across G7 FX)
    # usd_spec_crowding_z = -mean(net_z(G7 FX))
    # -----------------------------------------------------------------
    fx_g7_net_z = _mean_net_z(df, FX_G7, trader_group="LEVERAGED_FUNDS")
    usd_spec_crowding_z = (-fx_g7_net_z).rename("usd_spec_crowding_z")

    # -----------------------------------------------------------------
    # FACTOR 2: FX Spec Momentum Pressure
    # zscore(mean(delta_net_1w across G7 FX))
    # -----------------------------------------------------------------
    fx_delta = _mean_delta_net_1w(df, FX_G7, trader_group="LEVERAGED_FUNDS")
    fx_spec_momo_z = _zscore_series(fx_delta).rename("fx_spec_momo_z")

    # -----------------------------------------------------------------
    # FACTOR 3: UST Curve Steepening Pressure
    # ust_curve_steepen_z = mean(net_z(10Y, 30Y)) - mean(net_z(2Y))
    # -----------------------------------------------------------------
    ust_long_net_z = _mean_net_z(df, UST_LONG, trader_group="LEVERAGED_FUNDS")
    ust_short_net_z = _mean_net_z(df, UST_SHORT, trader_group="LEVERAGED_FUNDS")
    ust_curve_steepen_z = (ust_long_net_z - ust_short_net_z).rename(
        "ust_curve_steepen_z"
    )

    # -----------------------------------------------------------------
    # FACTOR 4: UST Duration Crowding
    # ust_duration_crowding_z = mean(net_z(10Y, 30Y))
    # -----------------------------------------------------------------
    ust_duration_crowding_z = ust_long_net_z.rename("ust_duration_crowding_z")

    # -----------------------------------------------------------------
    # FACTOR 5: Equity Index Spec Positioning (SPX/NDX)
    # eq_index_crowding_z = mean(net_z(EQ_SPX, EQ_NDX))
    # -----------------------------------------------------------------
    eq_index_crowding_z = _mean_net_z(df, EQ_INDEX, trader_group="LEVERAGED_FUNDS")
    eq_index_crowding_z = eq_index_crowding_z.rename("eq_index_crowding_z")

    # -----------------------------------------------------------------
    # FACTOR 6: Risk-On Composite
    # risk_on = avg( eq_index_crowding_z,
    #                net_z(AUD), net_z(CAD),
    #               -net_z(UST_10Y), -net_z(JPY) )
    # -----------------------------------------------------------------
    aud_z = _symbol_net_z(df, FX_AUD, trader_group="LEVERAGED_FUNDS")
    cad_z = _symbol_net_z(df, FX_CAD, trader_group="LEVERAGED_FUNDS")
    ust10_z = _symbol_net_z(df, UST_10Y, trader_group="LEVERAGED_FUNDS")
    jpy_z = _symbol_net_z(df, FX_JPY, trader_group="LEVERAGED_FUNDS")

    risk_df = pd.concat(
        [
            eq_index_crowding_z.rename("eq"),
            aud_z.rename("aud"),
            cad_z.rename("cad"),
            ust10_z.rename("ust10"),
            jpy_z.rename("jpy"),
        ],
        axis=1,
        join="outer",
    )

    # negative contributions: long bonds & JPY = risk-off
    risk_df["risk_on_cftc_factor"] = risk_df[["eq", "aud", "cad", "ust10", "jpy"]].assign(
        ust10=lambda x: -x["ust10"],
        jpy=lambda x: -x["jpy"],
    ).mean(axis=1)

    risk_on_cftc_factor = risk_df["risk_on_cftc_factor"].rename("risk_on_cftc_factor")

    # -----------------------------------------------------------------
    # FACTOR 7: Global Spec Pressure (All LF positions)
    # global_spec_pressure_z = zscore(mean(abs(net_z)) across all LF)
    # -----------------------------------------------------------------
    df_lf = df[df["trader_group"] == "LEVERAGED_FUNDS"].copy()
    spec_abs_mean = (
        df_lf.dropna(subset=["net_contracts_zscore_52w"])
        .groupby("as_of_date")["net_contracts_zscore_52w"]
        .apply(lambda x: x.abs().mean())
        .sort_index()
    )
    global_spec_pressure_z = _zscore_series(spec_abs_mean).rename(
        "global_spec_pressure_z"
    )

    # -----------------------------------------------------------------
    # FACTOR 8: Dealer Hedging Stress (Rates)
    # dealer_hedge_stress_rates = mean(-net_z(10Y, 30Y)) for DEALER
    # -----------------------------------------------------------------
    df_dealer = df[df["trader_group"] == "DEALER"].copy()
    dealer_rates = df_dealer[
        df_dealer["spine_symbol"].isin(UST_LONG)
    ].groupby("as_of_date")["net_contracts_zscore_52w"].mean()

    dealer_hedge_stress_rates = (-dealer_rates).rename("dealer_hedge_stress_rates")

    # -----------------------------------------------------------------
    # FACTOR 9: Dealer Hedging Stress (FX)
    # dealer_hedge_stress_fx = mean(-net_z(FX_G7)) for DEALER
    # -----------------------------------------------------------------
    dealer_fx = df_dealer[
        df_dealer["spine_symbol"].isin(FX_G7)
    ].groupby("as_of_date")["net_contracts_zscore_52w"].mean()

    dealer_hedge_stress_fx = (-dealer_fx).rename("dealer_hedge_stress_fx")

    # -----------------------------------------------------------------
    # FACTOR 10: Cross-Asset Momentum of Spec Positioning
    # cross_asset_spec_momo_z = zscore(mean(delta_net_1w) across all LF)
    # -----------------------------------------------------------------
    cross_delta = (
        df_lf.groupby("as_of_date")["delta_net_contracts_1w"]
        .mean()
        .sort_index()
    )
    cross_asset_spec_momo_z = _zscore_series(cross_delta).rename(
        "cross_asset_spec_momo_z"
    )

    # -----------------------------------------------------------------
    # Assemble factor DataFrame
    # -----------------------------------------------------------------
    factors = pd.DataFrame(index=df["as_of_date"].sort_values().unique())
    factors.index.name = "as_of_date"

    series_list = [
        usd_spec_crowding_z,
        fx_spec_momo_z,
        ust_curve_steepen_z,
        ust_duration_crowding_z,
        eq_index_crowding_z,
        risk_on_cftc_factor,
        global_spec_pressure_z,
        dealer_hedge_stress_rates,
        dealer_hedge_stress_fx,
        cross_asset_spec_momo_z,
    ]

    for s in series_list:
        factors[s.name] = s

    factors = factors.sort_index()
    factors["source_report_type"] = "TFF_FUT_ONLY"
    ts_now = pd.Timestamp.utcnow()
    factors["created_at_utc"] = ts_now
    factors["updated_at_utc"] = ts_now

    factors = factors.reset_index()
    log.info("[COT-FACTORS] Built factor leaf. Rows=%d", len(factors))
    return factors


# ---------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------


def write_factors_to_disk_and_r2(df: pd.DataFrame, cfg: CotFactorConfig) -> None:
    os.makedirs(os.path.dirname(cfg.output_local_path), exist_ok=True)
    print("DEBUG: writing COT factors to", cfg.output_local_path)
    df.to_parquet(cfg.output_local_path, index=False)
    log.info("[COT-FACTORS] Wrote factors to %s", cfg.output_local_path)

    upload_file_to_r2(cfg.output_local_path, cfg.r2_key)
    log.info("[COT-FACTORS] Uploaded factors to R2 key=%s", cfg.r2_key)


# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------


def main(argv: List[str] | None = None) -> int:
    log.info("[SCRIPT-COT-FACTORS] Building COT factor leaf …")
    cfg = CotFactorConfig()
    df_factors = build_cot_factors(cfg)
    write_factors_to_disk_and_r2(df_factors, cfg)
    log.info("[SCRIPT-COT-FACTORS] Done – factors built successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))


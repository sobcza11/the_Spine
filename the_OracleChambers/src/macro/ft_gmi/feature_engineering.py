from __future__ import annotations

import pandas as pd
from macro.ft_gmi.lag_optimizer import find_optimal_lag


COMPONENT_COLS = [
    "rates_stress",
    "fx_stress",
    "energy_stress",
    "equity_stress",
    "credit_stress",
]

def build_features(df):

    df = df.copy()

    df["ft_gmi_change_1d"] = df["ft_gmi_score"].diff()
    df["ft_gmi_change_5d"] = df["ft_gmi_score"].diff(5)

    df["ft_gmi_ma_5"] = df["ft_gmi_score"].rolling(5).mean()
    df["ft_gmi_ma_20"] = df["ft_gmi_score"].rolling(20).mean()

    df["stress_acceleration"] = df["ft_gmi_change_5d"].diff()

    return df


def add_ft_gmi_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out = out.sort_values("date").reset_index(drop=True)

    if "ft_gmi_score" in out.columns:
        out["ft_gmi_dod"] = out["ft_gmi_score"].diff()
        out["ft_gmi_vol_20d"] = out["ft_gmi_score"].rolling(20, min_periods=5).std()
        out["ft_gmi_score_fwd45d"] = out["ft_gmi_score"].shift(-45)

    for col in COMPONENT_COLS:
        if col in out.columns:
            out[f"{col}_dod"] = out[col].diff()
            out[f"{col}_vol_20d"] = out[col].rolling(20, min_periods=5).std()
            out[f"{col}_fwd45d"] = out[col].shift(-45)

    present = [c for c in COMPONENT_COLS if c in out.columns]
    if present:
        out["max_component"] = out[present].max(axis=1)
        out["min_component"] = out[present].min(axis=1)
        out["dispersion_score"] = out["max_component"] - out["min_component"]
        out["top_driver"] = out[present].idxmax(axis=1)
        out["top_driver_score"] = out[present].max(axis=1)

    return out

def compute_optimal_lags(df):

    signals = [
        "rates_stress",
        "fx_stress",
        "energy_stress",
        "equity_stress",
        "credit_stress"
    ]

    results = {}

    for s in signals:
        lag, corr = find_optimal_lag(df[s], df["ft_gmi_score"])
        results[s] = (lag, corr)

    return results


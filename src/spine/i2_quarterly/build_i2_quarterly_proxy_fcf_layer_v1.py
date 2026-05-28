from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd
import numpy as np


def classify_proxy_state(x):

    if x >= 0.80:
        return "systemic_proxy_fcf_pressure"

    if x >= 0.65:
        return "fragile_proxy_fcf_pressure"

    if x >= 0.50:
        return "elevated_proxy_fcf_pressure"

    if x >= 0.35:
        return "watch_proxy_fcf_pressure"

    return "stable_proxy_fcf_state"


def build_i2_quarterly_proxy_fcf_layer_v1():

    root = Path.cwd()

    src = (
        root
        / "data"
        / "i2_quarterly"
        / "i2_quarterly_fcf_upgrade_v1.parquet"
    )

    out = root / "data" / "i2_quarterly"

    if not src.exists():
        raise FileNotFoundError(f"Missing source file: {src}")

    df = pd.read_parquet(src).copy()

    df["statement_date"] = pd.to_datetime(
        df["statement_date"],
        errors="coerce"
    )

    df = (
        df
        .sort_values(["symbol", "statement_date"])
        .reset_index(drop=True)
    )

    # =====================================================
    # PROXY COMPONENTS
    # =====================================================

    cash_col = "Cash, Cash Equivalents & Short Term Investments"
    ppe_col = "Property, Plant & Equipment, Net"

    if cash_col not in df.columns:
        raise KeyError(f"Missing cash column: {cash_col}")

    if ppe_col not in df.columns:
        raise KeyError(f"Missing PPE column: {ppe_col}")

    df["q_cash_balance"] = pd.to_numeric(
        df[cash_col],
        errors="coerce"
    )

    df["q_ppe_balance"] = pd.to_numeric(
        df[ppe_col],
        errors="coerce"
    )

    # =====================================================
    # CASH DELTA
    # =====================================================

    df["q_cash_delta"] = (
        df
        .groupby("symbol")["q_cash_balance"]
        .diff()
    )

    # =====================================================
    # PPE DELTA (CAPEX PROXY)
    # =====================================================

    df["q_ppe_delta"] = (
        df
        .groupby("symbol")["q_ppe_balance"]
        .diff()
    )

    # =====================================================
    # PROXY FCF
    # =====================================================

    df["q_fcf_proxy"] = (
        df["q_cash_delta"]
        - df["q_ppe_delta"].clip(lower=0)
    )

    # =====================================================
    # PROXY MOMENTUM
    # =====================================================

    df["q_fcf_proxy_change"] = (
        df
        .groupby("symbol")["q_fcf_proxy"]
        .diff()
    )

    df["q_fcf_proxy_momentum"] = (
        df["q_fcf_proxy_change"]
        .fillna(0)
    )

    # =====================================================
    # PROXY PRESSURE
    # =====================================================

    proxy_cap = (
        df["q_fcf_proxy_momentum"]
        .abs()
        .quantile(0.95)
    )

    if pd.notna(proxy_cap) and proxy_cap > 0:

        df["q_fcf_proxy_pressure"] = (
            (
                -df["q_fcf_proxy_momentum"]
            )
            .clip(lower=0, upper=proxy_cap)
            / proxy_cap
        ).fillna(0).round(4)

    else:

        df["q_fcf_proxy_pressure"] = 0.0

    # =====================================================
    # SURVIVABILITY DRIFT V3
    # =====================================================

    survivability_cols = [
        "q_recursive_drift_2q",
        "q_transition_pressure_1_4",
        "q_fcf_proxy_pressure",
    ]

    df["q_survivability_drift_v3"] = (
        df[survivability_cols]
        .mean(axis=1)
        .fillna(0)
        .round(4)
    )

    df["q_survivability_state_v3"] = (
        df["q_survivability_drift_v3"]
        .apply(classify_proxy_state)
    )

    # =====================================================
    # SUMMARY
    # =====================================================

    summary = {
        "component": "i2_quarterly_proxy_fcf_layer_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "rows": int(len(df)),
        "symbols": int(df["symbol"].nunique()),
        "average_proxy_fcf_pressure": round(
            float(df["q_fcf_proxy_pressure"].mean()),
            4
        ),
        "average_survivability_drift_v3": round(
            float(df["q_survivability_drift_v3"].mean()),
            4
        ),
        "status": "quarterly_proxy_fcf_layer_complete",
    }

    # =====================================================
    # SAVE
    # =====================================================

    parquet_path = (
        out
        / "i2_quarterly_proxy_fcf_layer_v1.parquet"
    )

    df.to_parquet(
        parquet_path,
        index=False
    )

    df.head(500).to_json(
        out / "i2_quarterly_proxy_fcf_layer_sample_v1.json",
        orient="records",
        indent=2,
        date_format="iso"
    )

    with open(
        out / "i2_quarterly_proxy_fcf_layer_summary_v1.json",
        "w",
        encoding="utf-8"
    ) as f:
        json.dump(summary, f, indent=2)

    print("I2 Quarterly Proxy FCF Layer complete")
    print("Rows:", summary["rows"])
    print("Symbols:", summary["symbols"])
    print("Average proxy FCF pressure:", summary["average_proxy_fcf_pressure"])
    print("Average survivability drift v3:", summary["average_survivability_drift_v3"])


if __name__ == "__main__":
    build_i2_quarterly_proxy_fcf_layer_v1()

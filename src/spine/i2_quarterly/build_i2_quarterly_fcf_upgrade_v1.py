from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd
import numpy as np


def find_col(df, candidates):

    cols = {c.lower(): c for c in df.columns}

    for candidate in candidates:

        if candidate.lower() in cols:
            return cols[candidate.lower()]

    for c in df.columns:

        lc = c.lower()

        for candidate in candidates:

            if candidate.lower() in lc:
                return c

    return None


def build_i2_quarterly_fcf_upgrade_v1():

    root = Path.cwd()

    src = (
        root
        / "data"
        / "i2_quarterly"
        / "i2_quarterly_transition_tasks_5_8_v1.parquet"
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
    # DISCOVER CFO + CAPEX COLUMNS
    # =====================================================

    cfo_candidates = [
        "Net Cash From Operating Activities",
        "Cash From Operating Activities",
        "Operating Cash Flow",
        "NetCashFromOperatingActivities",
        "CashFromOperations",
    ]

    capex_candidates = [
        "Capital Expenditures",
        "CapEx",
        "Purchase Of Property Plant Equipment",
        "PurchaseOfPropertyPlantEquipment",
    ]

    cfo_col = find_col(df, cfo_candidates)
    capex_col = find_col(df, capex_candidates)

    # =====================================================
    # CONSTRUCT FCF
    # =====================================================

    if cfo_col is not None:

        df["q_operating_cash_flow"] = pd.to_numeric(
            df[cfo_col],
            errors="coerce"
        )

    else:

        df["q_operating_cash_flow"] = np.nan

    if capex_col is not None:

        df["q_capex"] = (
            pd.to_numeric(
                df[capex_col],
                errors="coerce"
            )
            .abs()
        )

    else:

        df["q_capex"] = 0.0

    df["q_free_cash_flow"] = (
        df["q_operating_cash_flow"]
        - df["q_capex"]
    )

    # =====================================================
    # FCF MOMENTUM
    # =====================================================

    df["q_fcf_change"] = (
        df
        .groupby("symbol")["q_free_cash_flow"]
        .diff()
    )

    df["q_fcf_momentum"] = (
        df["q_fcf_change"]
        .fillna(0)
    )

    # =====================================================
    # FCF PRESSURE
    # =====================================================

    fcf_cap = (
        df["q_fcf_momentum"]
        .abs()
        .quantile(0.95)
    )

    if pd.notna(fcf_cap) and fcf_cap > 0:

        df["q_fcf_pressure"] = (
            (
                -df["q_fcf_momentum"]
            )
            .clip(lower=0, upper=fcf_cap)
            / fcf_cap
        ).fillna(0).round(4)

    else:

        df["q_fcf_pressure"] = 0.0

    # =====================================================
    # SURVIVABILITY DRIFT UPGRADE
    # =====================================================

    survivability_cols = [
        "q_recursive_drift_2q",
        "q_transition_pressure_1_4",
        "q_fcf_pressure",
    ]

    df["q_survivability_drift_v2"] = (
        df[survivability_cols]
        .mean(axis=1)
        .fillna(0)
        .round(4)
    )

    # =====================================================
    # STATE CLASSIFICATION
    # =====================================================

    def classify(x):

        if x >= 0.80:
            return "systemic_survivability_breakdown"

        if x >= 0.65:
            return "fragile_survivability_state"

        if x >= 0.50:
            return "elevated_survivability_pressure"

        if x >= 0.35:
            return "watch_survivability_pressure"

        return "stable_survivability_state"

    df["q_survivability_state_v2"] = (
        df["q_survivability_drift_v2"]
        .apply(classify)
    )

    # =====================================================
    # SUMMARY
    # =====================================================

    summary = {
        "component": "i2_quarterly_fcf_upgrade_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "rows": int(len(df)),
        "symbols": int(df["symbol"].nunique()),
        "cfo_column_detected": cfo_col,
        "capex_column_detected": capex_col,
        "average_fcf_pressure": round(
            float(df["q_fcf_pressure"].mean()),
            4
        ),
        "average_survivability_drift_v2": round(
            float(df["q_survivability_drift_v2"].mean()),
            4
        ),
        "status": "quarterly_fcf_upgrade_complete",
    }

    # =====================================================
    # SAVE
    # =====================================================

    parquet_path = (
        out
        / "i2_quarterly_fcf_upgrade_v1.parquet"
    )

    df.to_parquet(
        parquet_path,
        index=False
    )

    df.head(500).to_json(
        out / "i2_quarterly_fcf_upgrade_sample_v1.json",
        orient="records",
        indent=2,
        date_format="iso"
    )

    with open(
        out / "i2_quarterly_fcf_upgrade_summary_v1.json",
        "w",
        encoding="utf-8"
    ) as f:
        json.dump(summary, f, indent=2)

    print("I2 Quarterly FCF Upgrade complete")
    print("Rows:", summary["rows"])
    print("Symbols:", summary["symbols"])
    print("CFO column:", summary["cfo_column_detected"])
    print("CapEx column:", summary["capex_column_detected"])
    print("Average FCF pressure:", summary["average_fcf_pressure"])
    print("Average survivability drift v2:", summary["average_survivability_drift_v2"])


if __name__ == "__main__":
    build_i2_quarterly_fcf_upgrade_v1()

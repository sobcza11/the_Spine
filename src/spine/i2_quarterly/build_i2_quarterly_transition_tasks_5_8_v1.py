from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd
import numpy as np


def classify_state(x):

    if x >= 0.80:
        return "systemic_recursive_deterioration"

    if x >= 0.65:
        return "fragile_recursive_deterioration"

    if x >= 0.50:
        return "elevated_recursive_transition"

    if x >= 0.35:
        return "watch_recursive_transition"

    return "stable_recursive_transition"


def build_i2_quarterly_transition_tasks_5_8_v1():

    root = Path.cwd()

    src = (
        root
        / "data"
        / "i2_quarterly"
        / "i2_quarterly_transition_tasks_1_4_v1.parquet"
    )

    out = root / "data" / "i2_quarterly"
    out.mkdir(parents=True, exist_ok=True)

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
    # TASK 5 — Q/Q FCF MOMENTUM ENGINE
    # =====================================================

    fcf_candidates = [
        "free_cash_flow",
        "fcf",
        "net_cash_from_operating_activities",
        "operating_cash_flow",
    ]

    fcf_col = None

    for c in df.columns:
        if c.lower() in [x.lower() for x in fcf_candidates]:
            fcf_col = c
            break

    if fcf_col is not None:

        df["q_fcf"] = pd.to_numeric(
            df[fcf_col],
            errors="coerce"
        )

        df["q_fcf_change"] = (
            df
            .groupby("symbol")["q_fcf"]
            .diff()
        )

        df["q_fcf_momentum"] = (
            df["q_fcf_change"]
            .fillna(0)
        )

    else:

        df["q_fcf"] = np.nan
        df["q_fcf_change"] = np.nan
        df["q_fcf_momentum"] = 0.0

    # =====================================================
    # TASK 6 — 2Q RECURSIVE DRIFT ENGINE
    # =====================================================

    pressure_cols = [
        "q_debt_acceleration_pressure_norm",
        "q_margin_compression_norm",
        "q_liquidity_deterioration_norm",
        "q_interest_coverage_drift_norm",
    ]

    df["q_recursive_drift_2q"] = (
        df[pressure_cols]
        .mean(axis=1)
        .rolling(2, min_periods=1)
        .mean()
        .fillna(0)
        .round(4)
    )

    # =====================================================
    # TASK 7 — QUARTERLY SURVIVABILITY DRIFT
    # =====================================================

    survivability_components = [
        "q_recursive_drift_2q",
        "q_transition_pressure_1_4",
    ]

    if "q_fcf_momentum" in df.columns:

        fcf_abs = (
            df["q_fcf_momentum"]
            .abs()
            .quantile(0.95)
        )

        if pd.notna(fcf_abs) and fcf_abs > 0:

            df["q_fcf_pressure"] = (
                (
                    -df["q_fcf_momentum"]
                )
                .clip(lower=0, upper=fcf_abs)
                / fcf_abs
            ).fillna(0).round(4)

        else:
            df["q_fcf_pressure"] = 0.0

        survivability_components.append(
            "q_fcf_pressure"
        )

    df["q_survivability_drift"] = (
        df[survivability_components]
        .mean(axis=1)
        .fillna(0)
        .round(4)
    )

    # =====================================================
    # TASK 8 — RECURSIVE TRANSITION-STATE CLASSIFIER
    # =====================================================

    df["q_recursive_transition_state"] = (
        df["q_survivability_drift"]
        .apply(classify_state)
    )

    # =====================================================
    # SUMMARY
    # =====================================================

    summary = {
        "component": "i2_quarterly_transition_tasks_5_8_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "rows": int(len(df)),
        "symbols": int(df["symbol"].nunique()),
        "average_recursive_drift_2q": round(
            float(df["q_recursive_drift_2q"].mean()),
            4
        ),
        "average_survivability_drift": round(
            float(df["q_survivability_drift"].mean()),
            4
        ),
        "fcf_column_detected": fcf_col,
        "status": "i2_quarterly_transition_tasks_5_8_complete",
    }

    # =====================================================
    # SAVE
    # =====================================================

    parquet_path = (
        out
        / "i2_quarterly_transition_tasks_5_8_v1.parquet"
    )

    df.to_parquet(
        parquet_path,
        index=False
    )

    df.head(500).to_json(
        out / "i2_quarterly_transition_tasks_5_8_sample_v1.json",
        orient="records",
        indent=2,
        date_format="iso"
    )

    with open(
        out / "i2_quarterly_transition_tasks_5_8_summary_v1.json",
        "w",
        encoding="utf-8"
    ) as f:
        json.dump(summary, f, indent=2)

    print("I2 Quarterly Transition Tasks 5-8 complete")
    print("Rows:", summary["rows"])
    print("Symbols:", summary["symbols"])
    print("Average 2Q drift:", summary["average_recursive_drift_2q"])
    print("Average survivability drift:", summary["average_survivability_drift"])
    print("FCF column:", summary["fcf_column_detected"])


if __name__ == "__main__":
    build_i2_quarterly_transition_tasks_5_8_v1()

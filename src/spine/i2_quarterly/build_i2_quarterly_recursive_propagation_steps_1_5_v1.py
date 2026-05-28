from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd
import numpy as np


def classify_escalation(x):

    if x >= 0.80:
        return "systemic_survivability_escalation"

    if x >= 0.65:
        return "fragile_survivability_escalation"

    if x >= 0.50:
        return "elevated_survivability_escalation"

    if x >= 0.35:
        return "watch_survivability_escalation"

    return "stable_survivability_state"


def build_i2_quarterly_recursive_propagation_steps_1_5_v1():

    root = Path.cwd()

    src = (
        root
        / "data"
        / "i2_quarterly"
        / "i2_quarterly_proxy_fcf_layer_v1.parquet"
    )

    out = root / "data" / "i2_quarterly"

    if not src.exists():
        raise FileNotFoundError(f"Missing source: {src}")

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
    # STEP 1 — QUARTERLY PROPAGATION INJECTION LAYER
    # =====================================================

    df["q_recursive_propagation_pressure"] = (
        df["q_survivability_drift_v3"]
        * 1.15
    ).clip(upper=1.0).round(4)

    # =====================================================
    # STEP 2 — QUARTERLY FUSION PRESSURE OVERLAY
    # =====================================================

    df["q_fusion_pressure_overlay"] = (
        (
            df["q_transition_pressure_1_4"]
            + df["q_recursive_propagation_pressure"]
        ) / 2
    ).round(4)

    # =====================================================
    # STEP 3 — QUARTERLY GEOSCEN ESCALATION HOOK
    # =====================================================

    df["q_geoscen_escalation_hook"] = (
        (
            df["q_fusion_pressure_overlay"]
            + df["q_survivability_drift_v3"]
        ) / 2
    ).round(4)

    # =====================================================
    # STEP 4 — QUARTERLY EXECUTIVE SYNTHESIS LAYER
    # =====================================================

    df["q_executive_survivability_signal"] = (
        df["q_geoscen_escalation_hook"]
        .apply(classify_escalation)
    )

    # =====================================================
    # STEP 5 — RECURSIVE SURVIVABILITY PROPAGATION ENGINE
    # =====================================================

    df["q_recursive_survivability_propagation"] = (
        (
            df["q_recursive_propagation_pressure"]
            + df["q_geoscen_escalation_hook"]
            + df["q_fusion_pressure_overlay"]
        ) / 3
    ).round(4)

    # =====================================================
    # ESCALATION STATE
    # =====================================================

    df["q_recursive_survivability_state"] = (
        df["q_recursive_survivability_propagation"]
        .apply(classify_escalation)
    )

    # =====================================================
    # SUMMARY
    # =====================================================

    summary = {
        "component": "i2_quarterly_recursive_propagation_steps_1_5_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "rows": int(len(df)),
        "symbols": int(df["symbol"].nunique()),
        "average_recursive_propagation_pressure": round(
            float(df["q_recursive_propagation_pressure"].mean()),
            4
        ),
        "average_fusion_overlay": round(
            float(df["q_fusion_pressure_overlay"].mean()),
            4
        ),
        "average_geoscen_hook": round(
            float(df["q_geoscen_escalation_hook"].mean()),
            4
        ),
        "average_recursive_survivability_propagation": round(
            float(df["q_recursive_survivability_propagation"].mean()),
            4
        ),
        "status": "quarterly_recursive_propagation_steps_1_5_complete",
    }

    # =====================================================
    # SAVE
    # =====================================================

    parquet_path = (
        out
        / "i2_quarterly_recursive_propagation_steps_1_5_v1.parquet"
    )

    df.to_parquet(
        parquet_path,
        index=False
    )

    df.head(500).to_json(
        out / "i2_quarterly_recursive_propagation_steps_1_5_sample_v1.json",
        orient="records",
        indent=2,
        date_format="iso"
    )

    with open(
        out / "i2_quarterly_recursive_propagation_steps_1_5_summary_v1.json",
        "w",
        encoding="utf-8"
    ) as f:
        json.dump(summary, f, indent=2)

    print("I2 Quarterly Recursive Propagation Steps 1-5 complete")
    print("Rows:", summary["rows"])
    print("Symbols:", summary["symbols"])
    print("Avg propagation pressure:", summary["average_recursive_propagation_pressure"])
    print("Avg fusion overlay:", summary["average_fusion_overlay"])
    print("Avg GeoScen hook:", summary["average_geoscen_hook"])
    print("Avg survivability propagation:", summary["average_recursive_survivability_propagation"])


if __name__ == "__main__":
    build_i2_quarterly_recursive_propagation_steps_1_5_v1()

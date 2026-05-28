from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd
import numpy as np


def classify_escalation(x):
    if x >= 0.80:
        return "systemic_quarterly_systemicity"
    if x >= 0.65:
        return "fragile_quarterly_systemicity"
    if x >= 0.50:
        return "elevated_quarterly_systemicity"
    if x >= 0.35:
        return "watch_quarterly_systemicity"
    return "stable_quarterly_systemicity"


def build_i2_quarterly_recursive_propagation_steps_6_10_v1():
    root = Path.cwd()

    src = (
        root
        / "data"
        / "i2_quarterly"
        / "i2_quarterly_recursive_propagation_steps_1_5_v1.parquet"
    )

    out = root / "data" / "i2_quarterly"
    out.mkdir(parents=True, exist_ok=True)

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
    # STEP 6 — QUARTERLY CONTAGION SYNCHRONIZATION
    # =====================================================

    date_sync = (
        df.groupby("statement_date", as_index=False)
        .agg(
            q_avg_survivability_propagation=(
                "q_recursive_survivability_propagation",
                "mean"
            ),
            q_sync_symbol_count=("symbol", "nunique"),
            q_high_pressure_share=(
                "q_recursive_survivability_propagation",
                lambda x: float((x >= 0.35).mean())
            ),
        )
    )

    date_sync["q_contagion_synchronization"] = (
        (
            date_sync["q_avg_survivability_propagation"] * 0.70
            + date_sync["q_high_pressure_share"] * 0.30
        )
        .clip(upper=1.0)
        .round(4)
    )

    df = df.merge(
        date_sync[[
            "statement_date",
            "q_contagion_synchronization",
            "q_high_pressure_share",
        ]],
        on="statement_date",
        how="left"
    )

    # =====================================================
    # STEP 7 — SURVIVABILITY ESCALATION STATE ENGINE
    # =====================================================

    df["q_survivability_escalation_score"] = (
        (
            df["q_recursive_survivability_propagation"] * 0.70
            + df["q_contagion_synchronization"].fillna(0) * 0.30
        )
        .clip(upper=1.0)
        .round(4)
    )

    df["q_survivability_escalation_state"] = (
        df["q_survivability_escalation_score"]
        .apply(classify_escalation)
    )

    # =====================================================
    # STEP 8 — RECURSIVE CORPORATE FRAGILITY CONDITIONING
    # =====================================================

    df["q_corporate_fragility_conditioning"] = (
        (
            df["q_survivability_escalation_score"] * 0.60
            + df["q_fusion_pressure_overlay"] * 0.25
            + df["q_geoscen_escalation_hook"] * 0.15
        )
        .clip(upper=1.0)
        .round(4)
    )

    # =====================================================
    # STEP 9 — QUARTERLY SYSTEMICITY OVERLAY
    # =====================================================

    df["q_quarterly_systemicity_overlay"] = (
        (
            df["q_corporate_fragility_conditioning"] * 0.50
            + df["q_contagion_synchronization"].fillna(0) * 0.30
            + df["q_recursive_propagation_pressure"] * 0.20
        )
        .clip(upper=1.0)
        .round(4)
    )

    df["q_quarterly_systemicity_state"] = (
        df["q_quarterly_systemicity_overlay"]
        .apply(classify_escalation)
    )

    # =====================================================
    # STEP 10 — INSTITUTIONAL SURVIVABILITY TOPOLOGY LAYER
    # =====================================================

    topology = (
        df.groupby("statement_date", as_index=False)
        .agg(
            q_topology_pressure=("q_quarterly_systemicity_overlay", "mean"),
            q_topology_dispersion=("q_quarterly_systemicity_overlay", "std"),
            q_topology_symbols=("symbol", "nunique"),
            q_topology_high_pressure_share=(
                "q_quarterly_systemicity_overlay",
                lambda x: float((x >= 0.35).mean())
            ),
        )
    )

    topology["q_topology_dispersion"] = (
        topology["q_topology_dispersion"]
        .fillna(0)
        .round(4)
    )

    topology["q_institutional_survivability_topology_score"] = (
        (
            topology["q_topology_pressure"] * 0.50
            + topology["q_topology_dispersion"] * 0.25
            + topology["q_topology_high_pressure_share"] * 0.25
        )
        .clip(upper=1.0)
        .round(4)
    )

    topology["q_institutional_survivability_topology_state"] = (
        topology["q_institutional_survivability_topology_score"]
        .apply(classify_escalation)
    )

    latest_topology = (
        topology
        .sort_values("statement_date")
        .tail(1)
        .to_dict(orient="records")
    )

    # =====================================================
    # SUMMARY
    # =====================================================

    summary = {
        "component": "i2_quarterly_recursive_propagation_steps_6_10_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "rows": int(len(df)),
        "symbols": int(df["symbol"].nunique()),
        "date_count": int(df["statement_date"].nunique()),
        "average_contagion_synchronization": round(
            float(df["q_contagion_synchronization"].mean()),
            4
        ),
        "average_survivability_escalation_score": round(
            float(df["q_survivability_escalation_score"].mean()),
            4
        ),
        "average_corporate_fragility_conditioning": round(
            float(df["q_corporate_fragility_conditioning"].mean()),
            4
        ),
        "average_quarterly_systemicity_overlay": round(
            float(df["q_quarterly_systemicity_overlay"].mean()),
            4
        ),
        "latest_topology": latest_topology,
        "status": "quarterly_recursive_propagation_steps_6_10_complete",
    }

    # =====================================================
    # SAVE
    # =====================================================

    df.to_parquet(
        out / "i2_quarterly_recursive_propagation_steps_6_10_v1.parquet",
        index=False
    )

    df.head(500).to_json(
        out / "i2_quarterly_recursive_propagation_steps_6_10_sample_v1.json",
        orient="records",
        indent=2,
        date_format="iso"
    )

    topology.to_parquet(
        out / "i2_quarterly_survivability_topology_v1.parquet",
        index=False
    )

    topology.to_json(
        out / "i2_quarterly_survivability_topology_v1.json",
        orient="records",
        indent=2,
        date_format="iso"
    )

    with open(
        out / "i2_quarterly_recursive_propagation_steps_6_10_summary_v1.json",
        "w",
        encoding="utf-8"
    ) as f:
        json.dump(summary, f, indent=2, default=str)

    md = []
    md.append("# I2 Quarterly Recursive Propagation Steps 6-10")
    md.append("")
    md.append(f"Generated: {summary['generated_at_utc']}")
    md.append("")
    md.append("## Summary")
    md.append("")
    for k, v in summary.items():
        if k not in ["component", "latest_topology", "status"]:
            md.append(f"- {k}: {v}")

    md.append("")
    md.append("## Latest Topology")
    md.append("")
    md.append(pd.DataFrame(latest_topology).to_markdown(index=False))

    md.append("")
    md.append("## Bottom Line")
    md.append("")
    md.append(
        "Quarterly I2 deterioration now propagates into survivability escalation, corporate fragility conditioning, quarterly systemicity, and institutional survivability topology."
    )

    md_path = (
        out
        / "i2_quarterly_recursive_propagation_steps_6_10_v1.md"
    )

    md_path.write_text(
        "\n".join(md),
        encoding="utf-8"
    )

    print("I2 Quarterly Recursive Propagation Steps 6-10 complete")
    print("Rows:", summary["rows"])
    print("Symbols:", summary["symbols"])
    print("Dates:", summary["date_count"])
    print("Avg contagion sync:", summary["average_contagion_synchronization"])
    print("Avg survivability escalation:", summary["average_survivability_escalation_score"])
    print("Avg systemicity overlay:", summary["average_quarterly_systemicity_overlay"])
    print("OUTPUT:", md_path)


if __name__ == "__main__":
    build_i2_quarterly_recursive_propagation_steps_6_10_v1()

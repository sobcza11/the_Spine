from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd
import numpy as np


def classify_state(x):
    if x >= 0.80:
        return "systemic_survivability_intelligence"
    if x >= 0.65:
        return "fragile_survivability_intelligence"
    if x >= 0.50:
        return "elevated_survivability_intelligence"
    if x >= 0.35:
        return "watch_survivability_intelligence"
    return "stable_survivability_intelligence"


def build_i2_quarterly_survivability_intelligence_steps_6_10_v1():
    root = Path.cwd()

    src = root / "data" / "i2_quarterly" / "i2_quarterly_survivability_intelligence_steps_1_5_v1.parquet"
    out = root / "data" / "i2_quarterly"
    out.mkdir(parents=True, exist_ok=True)

    if not src.exists():
        raise FileNotFoundError(f"Missing source: {src}")

    df = pd.read_parquet(src).copy()
    df["statement_date"] = pd.to_datetime(df["statement_date"], errors="coerce")

    df = (
        df.dropna(subset=["symbol", "statement_date"])
        .sort_values(["symbol", "statement_date"])
        .reset_index(drop=True)
    )

    # =====================================================
    # STEP 6 — DYNAMIC SURVIVABILITY CONDITIONING ENGINE
    # =====================================================

    df["q_dynamic_survivability_conditioning"] = (
        (
            df["q_macro_corporate_recursive_coupling"] * 0.45
            + df["q_credit_cycle_survivability_pressure"] * 0.25
            + df["q_survivability_persistence_score"] * 0.20
            + df["q_sector_fragility_synchronization"].fillna(0) * 0.10
        )
        .clip(upper=1.0)
        .round(4)
    )

    df["q_dynamic_survivability_conditioning_state"] = (
        df["q_dynamic_survivability_conditioning"]
        .apply(classify_state)
    )

    # =====================================================
    # STEP 7 — RECURSIVE ANTI-FRAGILITY DETECTION ENGINE
    # =====================================================

    df["q_systemicity_change"] = (
        df.groupby("symbol")["q_quarterly_systemicity_overlay"]
        .diff()
        .fillna(0)
    )

    df["q_coupling_change"] = (
        df.groupby("symbol")["q_macro_corporate_recursive_coupling"]
        .diff()
        .fillna(0)
    )

    df["q_antifragility_score"] = (
        (
            (-df["q_systemicity_change"]).clip(lower=0) * 0.55
            + (-df["q_coupling_change"]).clip(lower=0) * 0.25
            + (1 - df["q_dynamic_survivability_conditioning"]).clip(lower=0) * 0.20
        )
        .clip(upper=1.0)
        .round(4)
    )

    df["q_antifragility_state"] = np.select(
        [
            df["q_antifragility_score"] >= 0.50,
            df["q_antifragility_score"] >= 0.25,
            df["q_antifragility_score"] >= 0.10,
        ],
        [
            "strong_recursive_antifragility",
            "emerging_recursive_antifragility",
            "watch_recursive_antifragility",
        ],
        default="no_antifragility_signal",
    )

    # =====================================================
    # STEP 8 — INSTITUTIONAL DETERIORATION GRAPH ENGINE
    # =====================================================

    edge_rows = []

    latest = (
        df.sort_values(["symbol", "statement_date"])
        .groupby("symbol", as_index=False)
        .tail(1)
    )

    for _, r in latest.iterrows():
        symbol = str(r["symbol"])
        pressure = float(r["q_dynamic_survivability_conditioning"])
        systemicity = float(r["q_quarterly_systemicity_overlay"])
        antifragility = float(r["q_antifragility_score"])

        edge_rows.append({
            "source_node": symbol,
            "target_node": "GeoScen",
            "edge_type": "corporate_survivability_conditioning",
            "edge_pressure": pressure,
        })

        edge_rows.append({
            "source_node": symbol,
            "target_node": "I2_Quarterly_Systemicity",
            "edge_type": "quarterly_systemicity_contribution",
            "edge_pressure": systemicity,
        })

        edge_rows.append({
            "source_node": symbol,
            "target_node": "I2_Antifragility",
            "edge_type": "antifragility_contribution",
            "edge_pressure": antifragility,
        })

    graph = pd.DataFrame(edge_rows)

    # =====================================================
    # STEP 9 — SURVIVABILITY REGIME CLASSIFICATION LAYER
    # =====================================================

    regime = (
        df.groupby("statement_date", as_index=False)
        .agg(
            q_regime_dynamic_conditioning=("q_dynamic_survivability_conditioning", "mean"),
            q_regime_systemicity=("q_quarterly_systemicity_overlay", "mean"),
            q_regime_antifragility=("q_antifragility_score", "mean"),
            q_regime_symbol_count=("symbol", "nunique"),
        )
    )

    regime["q_survivability_regime_score"] = (
        (
            regime["q_regime_dynamic_conditioning"] * 0.45
            + regime["q_regime_systemicity"] * 0.35
            + (1 - regime["q_regime_antifragility"]).clip(lower=0) * 0.20
        )
        .clip(upper=1.0)
        .round(4)
    )

    regime["q_survivability_regime_state"] = (
        regime["q_survivability_regime_score"]
        .apply(classify_state)
    )

    # =====================================================
    # STEP 10 — EXECUTIVE SURVIVABILITY ESCALATION LAYER
    # =====================================================

    latest_regime = regime.sort_values("statement_date").tail(1)

    executive = {
        "component": "i2_quarterly_executive_survivability_escalation_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "latest_statement_date": str(latest_regime["statement_date"].iloc[0]) if not latest_regime.empty else None,
        "latest_survivability_regime_score": round(float(latest_regime["q_survivability_regime_score"].iloc[0]), 4) if not latest_regime.empty else None,
        "latest_survivability_regime_state": str(latest_regime["q_survivability_regime_state"].iloc[0]) if not latest_regime.empty else None,
        "average_dynamic_survivability_conditioning": round(float(df["q_dynamic_survivability_conditioning"].mean()), 4),
        "average_antifragility_score": round(float(df["q_antifragility_score"].mean()), 4),
        "graph_edge_count": int(len(graph)),
        "symbol_count": int(df["symbol"].nunique()),
        "status": "quarterly_executive_survivability_escalation_complete",
    }

    summary = {
        "component": "i2_quarterly_survivability_intelligence_steps_6_10_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "rows": int(len(df)),
        "symbols": int(df["symbol"].nunique()),
        "dates": int(df["statement_date"].nunique()),
        "average_dynamic_survivability_conditioning": executive["average_dynamic_survivability_conditioning"],
        "average_antifragility_score": executive["average_antifragility_score"],
        "latest_survivability_regime_score": executive["latest_survivability_regime_score"],
        "latest_survivability_regime_state": executive["latest_survivability_regime_state"],
        "graph_edge_count": executive["graph_edge_count"],
        "status": "quarterly_survivability_intelligence_steps_6_10_complete",
    }

    # =====================================================
    # SAVE OUTPUTS
    # =====================================================

    df.to_parquet(
        out / "i2_quarterly_survivability_intelligence_steps_6_10_v1.parquet",
        index=False
    )

    df.head(500).to_json(
        out / "i2_quarterly_survivability_intelligence_steps_6_10_sample_v1.json",
        orient="records",
        indent=2,
        date_format="iso"
    )

    graph.to_parquet(
        out / "i2_quarterly_institutional_deterioration_graph_v1.parquet",
        index=False
    )

    graph.to_json(
        out / "i2_quarterly_institutional_deterioration_graph_v1.json",
        orient="records",
        indent=2
    )

    regime.to_parquet(
        out / "i2_quarterly_survivability_regime_v1.parquet",
        index=False
    )

    regime.to_json(
        out / "i2_quarterly_survivability_regime_v1.json",
        orient="records",
        indent=2,
        date_format="iso"
    )

    with open(
        out / "i2_quarterly_executive_survivability_escalation_v1.json",
        "w",
        encoding="utf-8"
    ) as f:
        json.dump(executive, f, indent=2, default=str)

    with open(
        out / "i2_quarterly_survivability_intelligence_steps_6_10_summary_v1.json",
        "w",
        encoding="utf-8"
    ) as f:
        json.dump(summary, f, indent=2, default=str)

    md = []
    md.append("# I2 Quarterly Survivability Intelligence Steps 6-10")
    md.append("")
    md.append(f"Generated: {summary['generated_at_utc']}")
    md.append("")
    md.append("## Summary")
    md.append("")
    for k, v in summary.items():
        if k not in ["component", "status"]:
            md.append(f"- {k}: {v}")
    md.append("")
    md.append("## Executive Survivability Escalation")
    md.append("")
    for k, v in executive.items():
        if k not in ["component", "status"]:
            md.append(f"- {k}: {v}")
    md.append("")
    md.append("## Bottom Line")
    md.append("")
    md.append(
        "Quarterly I2 now includes dynamic survivability conditioning, anti-fragility detection, deterioration graphing, regime classification, and executive survivability escalation."
    )

    md_path = out / "i2_quarterly_survivability_intelligence_steps_6_10_v1.md"
    md_path.write_text("\n".join(md), encoding="utf-8")

    print("I2 Quarterly Survivability Intelligence Steps 6-10 complete")
    print("Rows:", summary["rows"])
    print("Symbols:", summary["symbols"])
    print("Dates:", summary["dates"])
    print("Avg dynamic conditioning:", summary["average_dynamic_survivability_conditioning"])
    print("Avg antifragility:", summary["average_antifragility_score"])
    print("Latest regime:", summary["latest_survivability_regime_state"])
    print("Graph edges:", summary["graph_edge_count"])
    print("OUTPUT:", md_path)


if __name__ == "__main__":
    build_i2_quarterly_survivability_intelligence_steps_6_10_v1()

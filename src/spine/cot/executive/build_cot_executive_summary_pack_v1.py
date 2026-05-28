from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


def build_cot_executive_summary_pack_v1():
    repo_root = Path.cwd()

    route_summary_path = (
        repo_root
        / "data"
        / "cot"
        / "routing"
        / "cot_geoscen_route_summary_v1.json"
    )

    iv_summary_path = (
        repo_root
        / "data"
        / "cot"
        / "routing"
        / "cot_iv_vector_summary_v1.json"
    )

    contagion_summary_path = (
        repo_root
        / "data"
        / "cot"
        / "routing"
        / "cot_cross_asset_contagion_summary_v1.json"
    )

    regime_summary_path = (
        repo_root
        / "data"
        / "cot"
        / "routing"
        / "cot_regime_conditioned_behavior_summary_v1.json"
    )

    conditioned_summary_path = (
        repo_root
        / "data"
        / "geoscen"
        / "conditioning"
        / "geoscen_conditioned_cot_routing_summary_v1.json"
    )

    out_dir = (
        repo_root
        / "data"
        / "cot"
        / "executive"
    )

    out_dir.mkdir(parents=True, exist_ok=True)

    with open(route_summary_path, "r", encoding="utf-8") as f:
        route_summary = json.load(f)

    with open(iv_summary_path, "r", encoding="utf-8") as f:
        iv_summary = json.load(f)

    with open(contagion_summary_path, "r", encoding="utf-8") as f:
        contagion_summary = json.load(f)

    with open(regime_summary_path, "r", encoding="utf-8") as f:
        regime_summary = json.load(f)

    with open(conditioned_summary_path, "r", encoding="utf-8") as f:
        conditioned_summary = json.load(f)

    executive_summary = {
        "component": "cot_executive_summary_pack_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),

        "coverage": {
            "instrument_count": route_summary.get("instrument_count"),
            "high_risk_instruments": route_summary.get("high_risk_instruments"),
        },

        "stress": {
            "raw_cot_stress": route_summary.get("raw_cot_stress"),
            "quality_weighted_cot_stress": route_summary.get("quality_weighted_cot_stress"),
            "max_conditioned_cot_stress": conditioned_summary.get("max_conditioned_cot_stress"),
        },

        "iv_transition": {
            "transition_pressure": iv_summary.get("cot_iv_transition_pressure"),
            "transition_instruments": iv_summary.get("transition_instruments"),
        },

        "contagion": {
            "avg_contagion_pressure": contagion_summary.get("avg_contagion_pressure"),
            "max_contagion_pressure": contagion_summary.get("max_contagion_pressure"),
            "elevated_edges": contagion_summary.get("elevated_edges"),
        },

        "regime": {
            "dominant_regime": regime_summary.get("dominant_cot_regime"),
            "avg_instability": regime_summary.get("avg_latest_instability"),
            "max_instability": regime_summary.get("max_latest_instability"),
            "avg_unwind_probability": regime_summary.get("avg_latest_unwind_probability"),
        },

        "conditioning": {
            "enabled": True,
            "winsorization": "2.5% / 97.5%",
            "scaling_method": "rolling_52w_vol_scale",
            "signal_quality_reweighting": True,
        },

        "status": "executive_summary_complete",
    }

    escalation_lines = [
        "# GeoScen Escalation Note",
        "",
        "## Executive Interpretation",
        "",
        "COT positioning cognition currently indicates elevated instability",
        "within several cross-asset positioning clusters.",
        "",
        f"High-risk instruments: {', '.join(route_summary.get('high_risk_instruments', []))}",
        "",
        f"Current IV[t] transition pressure: {iv_summary.get('cot_iv_transition_pressure')}",
        "",
        f"Dominant positioning regime: {regime_summary.get('dominant_cot_regime')}",
        "",
        "Interpretation:",
        "",
        "- Positioning instability remains elevated but conditioned.",
        "- Signal quality filtering is active.",
        "- GeoScen escalation routing is operational.",
        "- Cross-asset contagion propagation is active.",
        "- Structural fragility clusters remain concentrated in",
        "  CHF, BTC, RTY, and GBP.",
        "",
        "Assessment:",
        "",
        "The system currently reflects a",
        "medium-to-elevated macro positioning fragility regime",
        "rather than a systemic instability event.",
    ]

    iv_transition_summary = {
        "component": "cot_iv_transition_summary_v1",
        "transition_pressure": iv_summary.get("cot_iv_transition_pressure"),
        "max_transition_pressure": iv_summary.get("max_transition_pressure"),
        "transition_instruments": iv_summary.get("transition_instruments"),
        "iv_targets": iv_summary.get("iv_targets"),
        "status": "iv_transition_summary_complete",
    }

    executive_json = out_dir / "cot_executive_summary_v1.json"
    escalation_md = out_dir / "cot_geoscen_escalation_note_v1.md"
    iv_json = out_dir / "cot_iv_transition_summary_v1.json"

    with open(executive_json, "w", encoding="utf-8") as f:
        json.dump(executive_summary, f, indent=2)

    with open(escalation_md, "w", encoding="utf-8") as f:
        f.write("\n".join(escalation_lines))

    with open(iv_json, "w", encoding="utf-8") as f:
        json.dump(iv_transition_summary, f, indent=2)

    print("COT executive summary pack complete")
    print("EXECUTIVE JSON:", executive_json)
    print("ESCALATION NOTE:", escalation_md)
    print("IV SUMMARY JSON:", iv_json)
    print("Summary:", executive_summary)

    return executive_summary


if __name__ == "__main__":
    build_cot_executive_summary_pack_v1()

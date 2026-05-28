from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd
import numpy as np


def read_json(path: Path):
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


def read_parquet(path: Path):
    if path.exists():
        return pd.read_parquet(path)
    return pd.DataFrame()


def classify(x):
    if x >= 0.70: return "systemic_geoscen_state"
    if x >= 0.55: return "fragile_geoscen_state"
    if x >= 0.40: return "elevated_geoscen_state"
    if x >= 0.25: return "watch_geoscen_state"
    return "stable_geoscen_state"


def build_geoscen_executive_synthesis_integration_v1():
    root = Path.cwd()
    out = root / "data" / "geoscen"
    out.mkdir(parents=True, exist_ok=True)

    fusion = read_json(root / "data" / "fusion" / "cross_engine_fusion_score_summary_v1.json")
    propagation = read_json(root / "data" / "propagation" / "institutional_recursive_coordination_layer_summary_v1.json")
    semantic = read_json(root / "data" / "narrative" / "institutional_semantic_runtime_summary_v1.json")
    governance = read_json(root / "data" / "narrative" / "phase4_semantic_governance_note_v1.json")
    executive = read_json(root / "data" / "executive" / "master_ecosystem_summary_v1.json")
    i2 = read_json(root / "data" / "i2" / "i2_top_bottom_company_report_summary_v1.json")

    fusion_pressure = float(fusion.get("fusion_pressure", 0))
    coordinated_pressure = float(propagation.get("average_coordinated_pressure", 0))
    semantic_pressure = float(semantic.get("average_semantic_pressure", 0))
    fragility_pressure = float(i2.get("average_fragility_pressure", 0))

    # =====================================================
    # GOVERNED SYNTHESIS WEIGHTING
    # =====================================================

    geoscen_pressure = round(
        (
            fusion_pressure * 0.40 +
            coordinated_pressure * 0.30 +
            fragility_pressure * 0.25 +
            semantic_pressure * 0.05
        ),
        4
    )

    geoscen_state = classify(geoscen_pressure)

    # =====================================================
    # EXECUTIVE INTERPRETATION
    # =====================================================

    if geoscen_pressure >= 0.70:
        executive_read = "Systemic recursive escalation pressure active across multiple structural engines."
    elif geoscen_pressure >= 0.55:
        executive_read = "Fragility escalation building across coordinated structural engines."
    elif geoscen_pressure >= 0.40:
        executive_read = "Elevated recursive monitoring warranted across multiple system domains."
    elif geoscen_pressure >= 0.25:
        executive_read = "Watch-level recursive escalation pressure detected with structural stability still dominant."
    else:
        executive_read = "Structural stability remains dominant despite localized recursive pressure."

    synthesis = {
        "component": "geoscen_executive_synthesis_integration_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),

        "geoscen_executive_state": {
            "geoscen_pressure": geoscen_pressure,
            "geoscen_state": geoscen_state,
            "executive_read": executive_read,
        },

        "source_pressures": {
            "fusion_pressure": fusion_pressure,
            "coordinated_recursive_pressure": coordinated_pressure,
            "i2_fragility_pressure": fragility_pressure,
            "semantic_pressure": semantic_pressure,
        },

        "governance_alignment": {
            "cpmai_alignment": True,
            "transformer_inference_enabled": governance.get("governance_position", {}).get("transformer_inference_enabled"),
            "semantic_override_allowed": governance.get("governance_position", {}).get("semantic_override_allowed"),
            "structure_remains_source_of_truth": governance.get("governance_position", {}).get("structure_remains_source_of_truth"),
        },

        "system_context": {
            "institutional_readiness": executive.get("current_maturity_read", {}).get("institutional_readiness"),
            "architecture_maturity": executive.get("current_maturity_read", {}).get("architecture_maturity"),
            "production_maturity": executive.get("current_maturity_read", {}).get("production_maturity"),
        },

        "executive_actions": [
            "Continue replacing scaffold pressure with validated live inputs",
            "Expand recursive memory persistence",
            "Validate semantic pressure against historical macro regimes",
            "Maintain semantic governance restrictions until backtesting is complete",
            "Increase GeoScen executive synthesis integration with IV[t] transition topology",
        ],

        "status": "geoscen_executive_synthesis_integration_complete",
    }

    json_path = out / "geoscen_executive_synthesis_integration_v1.json"
    md_path = out / "geoscen_executive_synthesis_integration_v1.md"

    json_path.write_text(json.dumps(synthesis, indent=2), encoding="utf-8")

    md = []
    md.append("# GeoScen Executive Synthesis Integration")
    md.append("")
    md.append(f"Generated: {synthesis['generated_at_utc']}")
    md.append("")
    md.append("## Executive State")
    md.append("")
    md.append(f"- GeoScen pressure: {geoscen_pressure}")
    md.append(f"- GeoScen state: {geoscen_state}")
    md.append(f"- Executive read: {executive_read}")
    md.append("")
    md.append("## Source Pressures")
    md.append("")
    for k, v in synthesis["source_pressures"].items():
        md.append(f"- {k}: {v}")
    md.append("")
    md.append("## Governance Alignment")
    md.append("")
    for k, v in synthesis["governance_alignment"].items():
        md.append(f"- {k}: {v}")
    md.append("")
    md.append("## System Context")
    md.append("")
    for k, v in synthesis["system_context"].items():
        md.append(f"- {k}: {v}")
    md.append("")
    md.append("## Executive Actions")
    md.append("")
    for item in synthesis["executive_actions"]:
        md.append(f"- {item}")
    md.append("")
    md.append("## Bottom Line")
    md.append("")
    md.append("GeoScen executive synthesis is now operational as a governed recursive escalation interpretation layer integrating structural cognition, recursive propagation, financial durability cognition, and constrained semantic context.")

    md_path.write_text("\n".join(md), encoding="utf-8")

    print("GeoScen Executive Synthesis Integration complete")
    print("GeoScen pressure:", geoscen_pressure)
    print("GeoScen state:", geoscen_state)
    print("OUTPUT:", md_path)


if __name__ == "__main__":
    build_geoscen_executive_synthesis_integration_v1()

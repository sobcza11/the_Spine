from pathlib import Path
from datetime import datetime, UTC
import json


def read_json(path: Path):
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


def build_phase4_semantic_governance_note_v1():
    root = Path.cwd()
    out = root / "data" / "narrative"
    executive_out = root / "data" / "executive"
    out.mkdir(parents=True, exist_ok=True)
    executive_out.mkdir(parents=True, exist_ok=True)

    semantic_runtime = read_json(out / "institutional_semantic_runtime_summary_v1.json")
    transformer = read_json(out / "transformer_escalation_layer_summary_v1.json")
    master = read_json(executive_out / "master_ecosystem_summary_v1.json")
    fusion = read_json(root / "data" / "fusion" / "cross_engine_fusion_score_summary_v1.json")

    note = {
        "component": "phase4_semantic_governance_note_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),

        "governance_position": {
            "semantic_layer_status": "governed_scaffold_active",
            "transformer_inference_enabled": bool(semantic_runtime.get("transformer_inference_enabled", False)),
            "semantic_override_allowed": bool(semantic_runtime.get("semantic_override_allowed", False)),
            "structure_remains_source_of_truth": True,
            "narrative_cognition_role": "secondary_interpretation_layer",
            "cpmai_alignment": "active",
        },

        "current_semantic_runtime": {
            "semantic_target_count": semantic_runtime.get("semantic_target_count"),
            "average_semantic_pressure": semantic_runtime.get("average_semantic_pressure"),
            "runtime_status": semantic_runtime.get("runtime_status"),
            "status": semantic_runtime.get("status"),
        },

        "transformer_control": {
            "inference_enabled": transformer.get("inference_enabled", False),
            "semantic_override_allowed": transformer.get("semantic_override_allowed", False),
            "approved_current_use": [
                "summarization",
                "executive interpretation",
                "governed narrative context",
                "human-readable synthesis",
            ],
            "not_approved_current_use": [
                "autonomous escalation override",
                "unvalidated causal inference",
                "ungoverned semantic signal creation",
                "black-box state transition control",
            ],
        },

        "structural_priority_order": [
            "FinState structural macro cognition",
            "I2 financial durability cognition",
            "COT positioning instability cognition",
            "RATES sovereign pressure cognition",
            "VinV valuation reflexivity cognition",
            "GeoScen escalation cognition",
            "IV[t] transition-state topology",
            "Narrative / semantic interpretation",
        ],

        "fusion_context": {
            "fusion_pressure": fusion.get("fusion_pressure"),
            "fusion_state": fusion.get("fusion_state"),
            "interpretation": "semantic layer contributes context but does not control fusion outcome",
        },

        "risk_controls": [
            "No semantic output may override structural registry outputs",
            "No transformer inference is treated as source of truth",
            "Narrative pressure must remain explainable and auditable",
            "Semantic escalation requires structural confirmation",
            "Human review remains required before any narrative-driven escalation",
        ],

        "future_activation_requirements": [
            "validated ISM commentary ingestion history",
            "semantic pressure backtesting",
            "narrative-state stability testing",
            "false-positive review against structural signals",
            "documented model selection and governance review",
            "explicit transformer activation flag",
        ],

        "status": "phase4_semantic_governance_note_complete",
    }

    json_path = out / "phase4_semantic_governance_note_v1.json"
    md_path = out / "phase4_semantic_governance_note_v1.md"
    executive_md_path = executive_out / "phase4_semantic_governance_note_v1.md"

    json_path.write_text(json.dumps(note, indent=2), encoding="utf-8")

    md = []
    md.append("# Phase 4 Semantic Governance Note")
    md.append("")
    md.append(f"Generated: {note['generated_at_utc']}")
    md.append("")
    md.append("## Governance Position")
    md.append("")
    md.append("- Semantic layer status: governed scaffold active")
    md.append(f"- Transformer inference enabled: {note['governance_position']['transformer_inference_enabled']}")
    md.append(f"- Semantic override allowed: {note['governance_position']['semantic_override_allowed']}")
    md.append("- Structure remains source of truth: True")
    md.append("- Narrative cognition role: secondary interpretation layer")
    md.append("- CPMAI alignment: active")
    md.append("")
    md.append("## Current Semantic Runtime")
    md.append("")
    for k, v in note["current_semantic_runtime"].items():
        md.append(f"- {k}: {v}")
    md.append("")
    md.append("## Transformer Control")
    md.append("")
    md.append(f"- Inference enabled: {note['transformer_control']['inference_enabled']}")
    md.append(f"- Semantic override allowed: {note['transformer_control']['semantic_override_allowed']}")
    md.append("")
    md.append("### Approved Current Use")
    md.append("")
    for item in note["transformer_control"]["approved_current_use"]:
        md.append(f"- {item}")
    md.append("")
    md.append("### Not Approved Current Use")
    md.append("")
    for item in note["transformer_control"]["not_approved_current_use"]:
        md.append(f"- {item}")
    md.append("")
    md.append("## Structural Priority Order")
    md.append("")
    for i, item in enumerate(note["structural_priority_order"], start=1):
        md.append(f"{i}. {item}")
    md.append("")
    md.append("## Fusion Context")
    md.append("")
    md.append(f"- Fusion pressure: {note['fusion_context']['fusion_pressure']}")
    md.append(f"- Fusion state: {note['fusion_context']['fusion_state']}")
    md.append(f"- Interpretation: {note['fusion_context']['interpretation']}")
    md.append("")
    md.append("## Risk Controls")
    md.append("")
    for item in note["risk_controls"]:
        md.append(f"- {item}")
    md.append("")
    md.append("## Future Activation Requirements")
    md.append("")
    for item in note["future_activation_requirements"]:
        md.append(f"- {item}")
    md.append("")
    md.append("## Bottom Line")
    md.append("")
    md.append("Phase 4 is active as semantic infrastructure, not autonomous semantic authority. Structural outputs remain the controlling source of truth.")

    md_text = "\n".join(md)

    md_path.write_text(md_text, encoding="utf-8")
    executive_md_path.write_text(md_text, encoding="utf-8")

    print("Phase 4 Semantic Governance Note complete")
    print("OUTPUT JSON:", json_path)
    print("OUTPUT MD:", md_path)
    print("EXECUTIVE COPY:", executive_md_path)
    print("Transformer inference enabled:", note["governance_position"]["transformer_inference_enabled"])
    print("Semantic override allowed:", note["governance_position"]["semantic_override_allowed"])


if __name__ == "__main__":
    build_phase4_semantic_governance_note_v1()

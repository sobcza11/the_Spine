from pathlib import Path
from datetime import datetime, UTC
import json


def classify_narrative_permission(governance_state, projected_state):
    if governance_state in ["governance_lockdown", "governance_throttle"]:
        return "narrative_restricted"

    if projected_state in [
        "projected_systemic_fragility",
        "projected_cascade_risk",
    ]:
        return "narrative_guarded"

    return "narrative_allowed"


def build_recursive_narrative_cognition_scaffold_v1():
    repo_root = Path.cwd()

    projection_path = (
        repo_root
        / "data"
        / "geoscen"
        / "projection"
        / "recursive_scenario_projection_summary_v1.json"
    )

    fusion_path = (
        repo_root
        / "data"
        / "geoscen"
        / "fusion"
        / "cross_domain_recursive_fusion_summary_v1.json"
    )

    regime_memory_path = (
        repo_root
        / "data"
        / "geoscen"
        / "fusion"
        / "recursive_regime_memory_summary_v1.json"
    )

    topology_path = (
        repo_root
        / "data"
        / "geoscen"
        / "recursive"
        / "recursive_geoscen_topology_summary_v1.json"
    )

    governance_path = (
        repo_root
        / "data"
        / "geoscen"
        / "recursive"
        / "geoscen_recursive_governance_summary_v1.json"
    )

    out_dir = (
        repo_root
        / "data"
        / "geoscen"
        / "narrative"
    )

    out_dir.mkdir(parents=True, exist_ok=True)

    with open(projection_path, "r", encoding="utf-8") as f:
        projection = json.load(f)

    with open(fusion_path, "r", encoding="utf-8") as f:
        fusion = json.load(f)

    with open(regime_memory_path, "r", encoding="utf-8") as f:
        regime_memory = json.load(f)

    with open(topology_path, "r", encoding="utf-8") as f:
        topology = json.load(f)

    with open(governance_path, "r", encoding="utf-8") as f:
        governance = json.load(f)

    governance_state = governance.get("governance_state", "unknown")
    projected_state = projection.get("highest_projected_state", "unknown")

    narrative_permission = classify_narrative_permission(
        governance_state,
        projected_state,
    )

    narrative_scaffold = {
        "component": "recursive_narrative_cognition_scaffold_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "status": "scaffold_only_no_llm_inference",
        "narrative_permission": narrative_permission,
        "governance_state": governance_state,
        "governance_action": governance.get("governance_action"),
        "projection": {
            "highest_projected_scenario": projection.get("highest_projected_scenario"),
            "highest_projected_state": projection.get("highest_projected_state"),
            "max_projected_systemic_score": projection.get("max_projected_systemic_score"),
            "max_projected_cascade_probability": projection.get("max_projected_cascade_probability"),
        },
        "fusion": {
            "systemic_recursive_state": fusion.get("systemic_recursive_state"),
            "cross_domain_recursive_pressure": fusion.get("cross_domain_recursive_pressure"),
            "highest_feedback_links": fusion.get("highest_feedback_links"),
            "domain_states": fusion.get("domain_states"),
        },
        "memory": {
            "regime_memory_score": regime_memory.get("regime_memory_score"),
            "regime_memory_state": regime_memory.get("regime_memory_state"),
            "regime_memory_run_count": regime_memory.get("regime_memory_run_count"),
        },
        "topology": {
            "recursive_topology_score": topology.get("recursive_topology_score"),
            "recursive_topology_state": topology.get("recursive_topology_state"),
            "interpretation": topology.get("interpretation"),
        },
        "approved_narrative_functions": [
            "executive_summary_generation",
            "scenario_explanation",
            "risk_language_translation",
            "recursive_state_interpretation",
            "evidence_linked_briefing",
        ],
        "not_approved": [
            "autonomous_trading_decisions",
            "unverified causal claims",
            "ungoverned LLM conclusions",
            "black-box escalation overrides",
            "narrative-led signal generation",
        ],
        "narrative_guardrails": {
            "structural_outputs_remain_source_of_truth": True,
            "llm_may_explain_but_not_override": True,
            "scenario_projection_required_before_narrative": True,
            "governance_state_required": True,
            "evidence_linking_required": True,
        },
    }

    narrative_prompt_template = {
        "component": "recursive_narrative_prompt_template_v1",
        "instruction": (
            "Generate an executive interpretation of the recursive GeoScen state "
            "using only approved structured outputs. Do not invent causality. "
            "Do not override quantitative state classifications."
        ),
        "required_inputs": [
            "recursive_scenario_projection_summary_v1.json",
            "cross_domain_recursive_fusion_summary_v1.json",
            "recursive_regime_memory_summary_v1.json",
            "recursive_geoscen_topology_summary_v1.json",
            "geoscen_recursive_governance_summary_v1.json",
        ],
        "output_sections": [
            "Current recursive state",
            "Primary recursive drivers",
            "Projected scenario path",
            "Governance interpretation",
            "Executive risk conclusion",
        ],
    }

    executive_note_lines = [
        "# Recursive Narrative Cognition Scaffold",
        "",
        "## Status",
        "",
        "Scaffold only. No LLM inference is activated.",
        "",
        "## Current Structural Interpretation",
        "",
        f"Highest projected scenario: {projection.get('highest_projected_scenario')}",
        f"Highest projected state: {projection.get('highest_projected_state')}",
        f"Cross-domain recursive pressure: {fusion.get('cross_domain_recursive_pressure')}",
        f"Regime memory state: {regime_memory.get('regime_memory_state')}",
        f"Recursive topology state: {topology.get('recursive_topology_state')}",
        f"Governance state: {governance_state}",
        "",
        "## Narrative Permission",
        "",
        narrative_permission,
        "",
        "## Guardrail",
        "",
        "Narrative cognition may explain structural outputs, but may not override them.",
    ]

    scaffold_json = out_dir / "recursive_narrative_cognition_scaffold_v1.json"
    prompt_json = out_dir / "recursive_narrative_prompt_template_v1.json"
    executive_md = out_dir / "recursive_narrative_cognition_scaffold_v1.md"

    with open(scaffold_json, "w", encoding="utf-8") as f:
        json.dump(narrative_scaffold, f, indent=2)

    with open(prompt_json, "w", encoding="utf-8") as f:
        json.dump(narrative_prompt_template, f, indent=2)

    with open(executive_md, "w", encoding="utf-8") as f:
        f.write("\n".join(executive_note_lines))

    print("Recursive narrative cognition scaffold complete")
    print("Narrative Permission:", narrative_permission)
    print("Status: scaffold_only_no_llm_inference")
    print("SCAFFOLD JSON:", scaffold_json)
    print("PROMPT TEMPLATE:", prompt_json)
    print("EXECUTIVE MD:", executive_md)
    print("Summary:", narrative_scaffold)

    return narrative_scaffold


if __name__ == "__main__":
    build_recursive_narrative_cognition_scaffold_v1()

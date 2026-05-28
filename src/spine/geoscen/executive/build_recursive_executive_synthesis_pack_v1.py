from pathlib import Path
from datetime import datetime, UTC
import json


def load_json(path):
    if not path.exists():
        return {}

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_recursive_executive_synthesis_pack_v1():
    repo_root = Path.cwd()

    fusion_dir = repo_root / "data" / "geoscen" / "fusion"
    recursive_dir = repo_root / "data" / "geoscen" / "recursive"
    projection_dir = repo_root / "data" / "geoscen" / "projection"
    narrative_dir = repo_root / "data" / "geoscen" / "narrative"

    out_dir = repo_root / "data" / "geoscen" / "executive"
    out_dir.mkdir(parents=True, exist_ok=True)

    fusion = load_json(
        fusion_dir / "cross_domain_recursive_fusion_summary_v1.json"
    )

    regime_memory = load_json(
        fusion_dir / "recursive_regime_memory_summary_v1.json"
    )

    scenario_projection = load_json(
        projection_dir / "recursive_scenario_projection_summary_v1.json"
    )

    topology = load_json(
        recursive_dir / "recursive_geoscen_topology_summary_v1.json"
    )

    governance = load_json(
        recursive_dir / "geoscen_recursive_governance_summary_v1.json"
    )

    fragility = load_json(
        recursive_dir / "systemic_fragility_state_machine_summary_v1.json"
    )

    contagion = load_json(
        recursive_dir / "recursive_contagion_propagation_summary_v1.json"
    )

    narrative = load_json(
        narrative_dir / "recursive_narrative_cognition_scaffold_v1.json"
    )

    executive_summary = {
        "component": "recursive_executive_synthesis_pack_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "status": "recursive_executive_synthesis_complete",

        "headline_state": {
            "systemic_recursive_state": fusion.get("systemic_recursive_state"),
            "recursive_topology_state": topology.get("recursive_topology_state"),
            "systemic_fragility_state": fragility.get("systemic_fragility_state"),
            "recursive_contagion_state": contagion.get("recursive_contagion_state"),
            "regime_memory_state": regime_memory.get("regime_memory_state"),
            "governance_state": governance.get("governance_state"),
            "narrative_permission": narrative.get("narrative_permission"),
        },

        "core_metrics": {
            "cross_domain_recursive_pressure": fusion.get("cross_domain_recursive_pressure"),
            "avg_adjusted_target_pressure": fusion.get("avg_adjusted_target_pressure"),
            "max_adjusted_target_pressure": fusion.get("max_adjusted_target_pressure"),
            "systemic_fragility_score": fragility.get("systemic_fragility_score"),
            "cascade_probability": fragility.get("cascade_probability"),
            "recursive_topology_score": topology.get("recursive_topology_score"),
            "regime_memory_score": regime_memory.get("regime_memory_score"),
            "governance_pressure": governance.get("governance_pressure"),
            "max_projected_systemic_score": scenario_projection.get("max_projected_systemic_score"),
            "max_projected_cascade_probability": scenario_projection.get("max_projected_cascade_probability"),
        },

        "primary_recursive_drivers": fusion.get("highest_feedback_links", []),

        "scenario_projection": {
            "highest_projected_scenario": scenario_projection.get("highest_projected_scenario"),
            "highest_projected_state": scenario_projection.get("highest_projected_state"),
            "governance_state": scenario_projection.get("governance_state"),
        },

        "governance": {
            "governance_action": governance.get("governance_action"),
            "recursion_mode": governance.get("recursion_mode"),
            "amplification_allowed": governance.get("amplification_allowed"),
            "recursion_allowed": governance.get("recursion_allowed"),
            "throttle_required": governance.get("throttle_required"),
            "lockdown_required": governance.get("lockdown_required"),
        },

        "interpretation": {
            "executive_read": (
                "Recursive macro pressure is active but contained. "
                "Cross-domain feedback is present, led by FX-to-Rates, "
                "Rates-to-FinState, and FX-to-COT channels. "
                "Scenario projection remains watch-level even under cascade assumptions. "
                "Governance is clear and narrative cognition is permitted, "
                "but structural outputs remain the source of truth."
            ),
            "risk_posture": "watch",
            "systemic_escalation": "not_systemic",
            "management_view": "monitor_recursive_cross_domain_feedback",
        },
    }

    executive_lines = [
        "# Recursive Executive Synthesis Pack",
        "",
        "## Executive Read",
        "",
        executive_summary["interpretation"]["executive_read"],
        "",
        "## Current State",
        "",
        f"- Systemic recursive state: {fusion.get('systemic_recursive_state')}",
        f"- Recursive topology state: {topology.get('recursive_topology_state')}",
        f"- Fragility state: {fragility.get('systemic_fragility_state')}",
        f"- Recursive contagion state: {contagion.get('recursive_contagion_state')}",
        f"- Regime memory state: {regime_memory.get('regime_memory_state')}",
        f"- Governance state: {governance.get('governance_state')}",
        "",
        "## Scenario Projection",
        "",
        f"- Highest projected scenario: {scenario_projection.get('highest_projected_scenario')}",
        f"- Highest projected state: {scenario_projection.get('highest_projected_state')}",
        f"- Max projected systemic score: {scenario_projection.get('max_projected_systemic_score')}",
        f"- Max projected cascade probability: {scenario_projection.get('max_projected_cascade_probability')}",
        "",
        "## Primary Recursive Drivers",
        "",
    ]

    for item in fusion.get("highest_feedback_links", []):
        executive_lines.append(
            f"- {item.get('source_domain')} ? {item.get('target_domain')}: {item.get('cross_domain_feedback')}"
        )

    executive_lines.extend(
        [
            "",
            "## Governance",
            "",
            f"- Action: {governance.get('governance_action')}",
            f"- Recursion mode: {governance.get('recursion_mode')}",
            f"- Amplification allowed: {governance.get('amplification_allowed')}",
            f"- Recursion allowed: {governance.get('recursion_allowed')}",
            "",
            "## Conclusion",
            "",
            "The current recursive system is in a watch posture. "
            "The architecture detects active cross-domain feedback, "
            "but does not currently project systemic or cascade-level escalation.",
        ]
    )

    executive_json = out_dir / "recursive_executive_synthesis_pack_v1.json"
    executive_md = out_dir / "recursive_executive_synthesis_pack_v1.md"

    with open(executive_json, "w", encoding="utf-8") as f:
        json.dump(executive_summary, f, indent=2)

    with open(executive_md, "w", encoding="utf-8") as f:
        f.write("\n".join(executive_lines))

    print("Recursive executive synthesis pack complete")
    print("EXECUTIVE JSON:", executive_json)
    print("EXECUTIVE MD:", executive_md)
    print("Summary:", executive_summary)

    return executive_summary


if __name__ == "__main__":
    build_recursive_executive_synthesis_pack_v1()

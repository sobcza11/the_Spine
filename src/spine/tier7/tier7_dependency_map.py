from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "tier7"
OUT_PATH = OUT_DIR / "tier7_dependency_map.json"


DEPENDENCY_MAP = {
    "institutional_cognition_operating_system": [
        "unified_institutional_cognition_kernel",
        "cross_runtime_state_federation",
        "persistent_executive_memory_os",
        "institutional_workflow_engine",
        "institutional_risk_command_center",
        "institutional_cognition_compiler",
        "executive_situational_awareness_theater",
    ],
    "institutional_cognition_compiler": [
        "global_macro_knowledge_graph",
        "continuous_historical_replay_engine",
        "live_institutional_telemetry_fabric",
        "cognitive_integrity_verification_engine",
        "institutional_trust_calibration_layer",
    ],
    "executive_situational_awareness_theater": [
        "dynamic_executive_briefing_generator",
        "institutional_risk_command_center",
        "sovereign_instability_monitoring_grid",
        "global_liquidity_intelligence_core",
        "regime_transition_early_warning_system",
    ],
    "institutional_workflow_engine": [
        "human_ai_executive_collaboration_layer",
        "institutional_deployment_governance_os",
        "macro_strategy_execution_orchestration",
        "autonomous_runtime_healing_systems",
    ],
    "global_macro_knowledge_graph": [
        "narrative_influence_mapping_engine",
        "global_transmission_topology_engine",
        "strategic_capital_flow_intelligence",
        "federated_cognition_expansion_layer",
    ],
}


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    dependency_count = sum(
        len(v) for v in DEPENDENCY_MAP.values()
    )

    payload = {
        "system": "IsoVector",
        "module": "tier7-dependency-map",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "dependency_map_enabled": True,

        "dependency_map": DEPENDENCY_MAP,

        "parent_component_count": len(DEPENDENCY_MAP),

        "dependency_count": dependency_count,

        "dependency_objective": (
            "Show how Tier 7 Institutional Operating System components connect across "
            "kernel, state, memory, workflow, risk, compiler, dashboard, telemetry, "
            "knowledge graph, and governance layers."
        ),

        "dependency_contract": {
            "os_root_defined": True,
            "compiler_dependencies_defined": True,
            "dashboard_dependencies_defined": True,
            "workflow_dependencies_defined": True,
            "knowledge_graph_dependencies_defined": True,
        },

        "governance": {
            "dependency_mapping_governed": True,
            "human_review_required": True,
            "llm_writeback_allowed": False,
            "audit_trail_required": True,
        },
    }

    OUT_PATH.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )

    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()

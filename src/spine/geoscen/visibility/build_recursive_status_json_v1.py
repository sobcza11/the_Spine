from pathlib import Path
from datetime import datetime, UTC
import json


def load_json(path):
    if not path.exists():
        return {}

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_recursive_status_json_v1():

    repo_root = Path.cwd()

    recursive_dir = (
        repo_root
        / "data"
        / "geoscen"
        / "recursive"
    )

    fusion_dir = (
        repo_root
        / "data"
        / "geoscen"
        / "fusion"
    )

    policy_dir = (
        repo_root
        / "data"
        / "geoscen"
        / "policy"
    )

    scenario_tree_dir = (
        repo_root
        / "data"
        / "geoscen"
        / "scenario_tree"
    )

    global_dir = (
        repo_root
        / "data"
        / "geoscen"
        / "global_expansion"
    )

    executive_dir = (
        repo_root
        / "data"
        / "geoscen"
        / "executive"
    )

    orchestration_dir = (
        repo_root
        / "data"
        / "geoscen"
        / "orchestration"
    )

    visibility_dir = (
        repo_root
        / "data"
        / "geoscen"
        / "visibility"
    )

    visibility_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    fragility = load_json(
        recursive_dir
        / "systemic_fragility_state_machine_summary_v1.json"
    )

    governance = load_json(
        recursive_dir
        / "geoscen_recursive_governance_summary_v1.json"
    )

    topology = load_json(
        recursive_dir
        / "recursive_geoscen_topology_summary_v1.json"
    )

    contagion = load_json(
        recursive_dir
        / "recursive_contagion_propagation_summary_v1.json"
    )

    fusion = load_json(
        fusion_dir
        / "cross_domain_recursive_fusion_summary_v1.json"
    )

    regime_memory = load_json(
        fusion_dir
        / "recursive_regime_memory_summary_v1.json"
    )

    policy = load_json(
        policy_dir
        / "recursive_policy_response_layer_summary_v1.json"
    )

    scenario_tree = load_json(
        scenario_tree_dir
        / "recursive_scenario_tree_generation_summary_v1.json"
    )

    global_expansion = load_json(
        global_dir
        / "global_recursive_ae_em_expansion_summary_v1.json"
    )

    executive = load_json(
        executive_dir
        / "recursive_executive_synthesis_pack_v1.json"
    )

    orchestration = load_json(
        orchestration_dir
        / "recursive_geoscen_orchestration_summary_v1.json"
    )

    runtime_state = {
        "component": "recursive_status_json_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),

        # =================================================
        # RUNTIME
        # =================================================

        "runtime": {
            "orchestration_status": orchestration.get("overall_status"),
            "pipeline_steps": orchestration.get("pipeline_steps"),
            "successful_steps": orchestration.get("successful_steps"),
            "failed_steps": orchestration.get("failed_steps"),
            "runtime_seconds_total": orchestration.get("runtime_seconds_total"),
        },

        # =================================================
        # SYSTEMIC STATE
        # =================================================

        "systemic_state": {
            "fragility_state": fragility.get("systemic_fragility_state"),
            "fragility_score": fragility.get("systemic_fragility_score"),
            "cascade_probability": fragility.get("cascade_probability"),
            "recursive_topology_state": topology.get("recursive_topology_state"),
            "recursive_topology_score": topology.get("recursive_topology_score"),
            "recursive_contagion_state": contagion.get("recursive_contagion_state"),
            "avg_recursive_contagion_pressure": contagion.get("avg_recursive_contagion_pressure"),
            "cross_domain_recursive_state": fusion.get("systemic_recursive_state"),
            "cross_domain_recursive_pressure": fusion.get("cross_domain_recursive_pressure"),
        },

        # =================================================
        # POLICY
        # =================================================

        "policy_state": {
            "policy_response_state": policy.get("policy_response_state"),
            "policy_response_pressure": policy.get("policy_response_pressure"),
            "dominant_policy_bias": policy.get("dominant_policy_bias"),
            "inflation_constraint_pressure": policy.get("inflation_constraint_pressure"),
            "growth_support_pressure": policy.get("growth_support_pressure"),
            "liquidity_stabilization_pressure": policy.get("liquidity_stabilization_pressure"),
        },

        # =================================================
        # MEMORY
        # =================================================

        "memory_state": {
            "regime_memory_state": regime_memory.get("regime_memory_state"),
            "regime_memory_score": regime_memory.get("regime_memory_score"),
            "regime_memory_run_count": regime_memory.get("regime_memory_run_count"),
        },

        # =================================================
        # GLOBAL
        # =================================================

        "global_state": {
            "global_recursive_state": global_expansion.get("global_recursive_state"),
            "global_recursive_pressure": global_expansion.get("global_recursive_pressure"),
            "regional_states": global_expansion.get("regional_states"),
            "highest_bridge_links": global_expansion.get("highest_bridge_links"),
        },

        # =================================================
        # SCENARIO TREE
        # =================================================

        "scenario_tree_state": {
            "highest_risk_branch": scenario_tree.get("highest_risk_branch"),
            "highest_risk_state": scenario_tree.get("highest_risk_state"),
            "max_branch_systemic_score": scenario_tree.get("max_branch_systemic_score"),
            "max_branch_cascade_probability": scenario_tree.get("max_branch_cascade_probability"),
            "branch_state_counts": scenario_tree.get("branch_state_counts"),
        },

        # =================================================
        # GOVERNANCE
        # =================================================

        "governance_state": {
            "governance_state": governance.get("governance_state"),
            "governance_pressure": governance.get("governance_pressure"),
            "governance_action": governance.get("governance_action"),
            "recursion_mode": governance.get("recursion_mode"),
            "amplification_allowed": governance.get("amplification_allowed"),
            "recursion_allowed": governance.get("recursion_allowed"),
            "throttle_required": governance.get("throttle_required"),
            "lockdown_required": governance.get("lockdown_required"),
        },

        # =================================================
        # EXECUTIVE
        # =================================================

        "executive_state": {
            "risk_posture": executive.get("interpretation", {}).get("risk_posture"),
            "systemic_escalation": executive.get("interpretation", {}).get("systemic_escalation"),
            "management_view": executive.get("interpretation", {}).get("management_view"),
            "executive_read": executive.get("interpretation", {}).get("executive_read"),
        },

        # =================================================
        # DOMINANT DRIVERS
        # =================================================

        "dominant_recursive_drivers": fusion.get(
            "highest_feedback_links",
            [],
        ),

        # =================================================
        # OVERALL STATE
        # =================================================

        "overall_runtime_state": {
            "runtime_mode": "offline_recursive_monitoring",
            "runtime_health": (
                "healthy"
                if orchestration.get("failed_steps", 0) == 0
                else "degraded"
            ),
            "systemic_risk_level": executive.get(
                "interpretation",
                {},
            ).get("risk_posture"),
            "global_recursive_mode": global_expansion.get(
                "global_recursive_state"
            ),
            "status": "recursive_visibility_runtime_active",
        },
    }

    status_path = (
        visibility_dir
        / "recursive_status_runtime_v1.json"
    )

    with open(status_path, "w", encoding="utf-8") as f:
        json.dump(
            runtime_state,
            f,
            indent=2,
        )

    print("Recursive status JSON complete")
    print("Runtime Health:", runtime_state["overall_runtime_state"]["runtime_health"])
    print("Systemic Risk Level:", runtime_state["overall_runtime_state"]["systemic_risk_level"])
    print("Global Recursive Mode:", runtime_state["overall_runtime_state"]["global_recursive_mode"])
    print("STATUS JSON:", status_path)
    print("Summary:", runtime_state["overall_runtime_state"])

    return runtime_state


if __name__ == "__main__":
    build_recursive_status_json_v1()

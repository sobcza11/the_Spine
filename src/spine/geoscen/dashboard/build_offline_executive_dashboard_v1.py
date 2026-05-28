from pathlib import Path
from datetime import datetime, UTC
import json


def load_json(path):

    if not path.exists():
        return {}

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_offline_executive_dashboard_v1():

    repo_root = Path.cwd()

    visibility_dir = (
        repo_root
        / "data"
        / "geoscen"
        / "visibility"
    )

    dashboard_dir = (
        repo_root
        / "data"
        / "geoscen"
        / "dashboard"
    )

    dashboard_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    runtime_status = load_json(
        visibility_dir
        / "recursive_status_runtime_v1.json"
    )

    dashboard_cache = load_json(
        visibility_dir
        / "dashboard_cache"
        / "recursive_dashboard_snapshot_v1.json"
    )

    timeline = load_json(
        visibility_dir
        / "timeline"
        / "recursive_timeline_engine_summary_v1.json"
    )

    registry = load_json(
        visibility_dir
        / "runtime_registry"
        / "runtime_state_registry_summary_v1.json"
    )

    dashboard_summary = dashboard_cache.get(
        "dashboard_summary",
        {}
    )

    overall_runtime = runtime_status.get(
        "overall_runtime_state",
        {}
    )

    governance = runtime_status.get(
        "governance_state",
        {}
    )

    systemic = runtime_status.get(
        "systemic_state",
        {}
    )

    scenario_tree = runtime_status.get(
        "scenario_tree_state",
        {}
    )

    global_state = runtime_status.get(
        "global_state",
        {}
    )

    policy = runtime_status.get(
        "policy_state",
        {}
    )

    executive = runtime_status.get(
        "executive_state",
        {}
    )

    dashboard = {

        "component": "offline_executive_dashboard_v1",

        "generated_at_utc": datetime.now(UTC).isoformat(),

        # =================================================
        # EXECUTIVE HEADER
        # =================================================

        "executive_header": {

            "runtime_health": overall_runtime.get(
                "runtime_health"
            ),

            "systemic_risk_level": overall_runtime.get(
                "systemic_risk_level"
            ),

            "global_recursive_mode": overall_runtime.get(
                "global_recursive_mode"
            ),

            "management_view": executive.get(
                "management_view"
            ),

            "executive_read": executive.get(
                "executive_read"
            ),
        },

        # =================================================
        # SYSTEMIC CONDITIONS
        # =================================================

        "systemic_conditions": {

            "fragility_state": systemic.get(
                "fragility_state"
            ),

            "fragility_score": systemic.get(
                "fragility_score"
            ),

            "cascade_probability": systemic.get(
                "cascade_probability"
            ),

            "recursive_contagion_state": systemic.get(
                "recursive_contagion_state"
            ),

            "cross_domain_recursive_state": systemic.get(
                "cross_domain_recursive_state"
            ),

            "cross_domain_recursive_pressure": systemic.get(
                "cross_domain_recursive_pressure"
            ),
        },

        # =================================================
        # POLICY CONDITIONS
        # =================================================

        "policy_conditions": {

            "policy_response_state": policy.get(
                "policy_response_state"
            ),

            "dominant_policy_bias": policy.get(
                "dominant_policy_bias"
            ),

            "policy_response_pressure": policy.get(
                "policy_response_pressure"
            ),

            "inflation_constraint_pressure": policy.get(
                "inflation_constraint_pressure"
            ),

            "growth_support_pressure": policy.get(
                "growth_support_pressure"
            ),

            "liquidity_stabilization_pressure": policy.get(
                "liquidity_stabilization_pressure"
            ),
        },

        # =================================================
        # GLOBAL CONDITIONS
        # =================================================

        "global_conditions": {

            "global_recursive_state": global_state.get(
                "global_recursive_state"
            ),

            "global_recursive_pressure": global_state.get(
                "global_recursive_pressure"
            ),

            "regional_states": global_state.get(
                "regional_states"
            ),

            "highest_bridge_links": global_state.get(
                "highest_bridge_links"
            ),
        },

        # =================================================
        # SCENARIO CONDITIONS
        # =================================================

        "scenario_conditions": {

            "highest_risk_branch": scenario_tree.get(
                "highest_risk_branch"
            ),

            "highest_risk_state": scenario_tree.get(
                "highest_risk_state"
            ),

            "max_branch_systemic_score": scenario_tree.get(
                "max_branch_systemic_score"
            ),

            "max_branch_cascade_probability": scenario_tree.get(
                "max_branch_cascade_probability"
            ),
        },

        # =================================================
        # TIMELINE CONDITIONS
        # =================================================

        "timeline_conditions": {

            "latest_transition_score": timeline.get(
                "latest_transition_score"
            ),

            "latest_transition_state": timeline.get(
                "latest_transition_state"
            ),

            "latest_recursive_direction": timeline.get(
                "latest_recursive_direction"
            ),

            "regime_shift_events": timeline.get(
                "regime_shift_events"
            ),
        },

        # =================================================
        # GOVERNANCE
        # =================================================

        "governance_conditions": {

            "governance_state": governance.get(
                "governance_state"
            ),

            "governance_action": governance.get(
                "governance_action"
            ),

            "recursion_mode": governance.get(
                "recursion_mode"
            ),

            "amplification_allowed": governance.get(
                "amplification_allowed"
            ),

            "throttle_required": governance.get(
                "throttle_required"
            ),

            "lockdown_required": governance.get(
                "lockdown_required"
            ),
        },

        # =================================================
        # DASHBOARD STATUS
        # =================================================

        "dashboard_status": {

            "dashboard_mode": "offline_institutional_monitor",

            "dashboard_state": "active",

            "runtime_registry_entries": registry.get(
                "registry_entries"
            ),

            "timeline_entries": timeline.get(
                "timeline_entries"
            ),

            "status": "offline_executive_dashboard_active",
        },
    }

    json_path = (
        dashboard_dir
        / "offline_executive_dashboard_v1.json"
    )

    md_path = (
        dashboard_dir
        / "offline_executive_dashboard_v1.md"
    )

    summary_path = (
        dashboard_dir
        / "offline_executive_dashboard_summary_v1.json"
    )

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(
            dashboard,
            f,
            indent=2,
        )

    md_lines = [

        "# GeoScen Offline Executive Dashboard",
        "",

        f"Generated UTC: {dashboard['generated_at_utc']}",
        "",

        "## Executive Summary",
        "",

        f"- Runtime Health: {dashboard['executive_header']['runtime_health']}",
        f"- Systemic Risk Level: {dashboard['executive_header']['systemic_risk_level']}",
        f"- Global Recursive Mode: {dashboard['executive_header']['global_recursive_mode']}",
        f"- Management View: {dashboard['executive_header']['management_view']}",
        "",

        "## Executive Read",
        "",
        dashboard['executive_header']['executive_read'],
        "",

        "## Systemic Conditions",
        "",

        f"- Fragility State: {dashboard['systemic_conditions']['fragility_state']}",
        f"- Recursive Contagion State: {dashboard['systemic_conditions']['recursive_contagion_state']}",
        f"- Cross-Domain Recursive State: {dashboard['systemic_conditions']['cross_domain_recursive_state']}",
        f"- Cascade Probability: {dashboard['systemic_conditions']['cascade_probability']}",
        "",

        "## Scenario Conditions",
        "",

        f"- Highest Risk Branch: {dashboard['scenario_conditions']['highest_risk_branch']}",
        f"- Highest Risk State: {dashboard['scenario_conditions']['highest_risk_state']}",
        f"- Max Branch Systemic Score: {dashboard['scenario_conditions']['max_branch_systemic_score']}",
        "",

        "## Governance",
        "",

        f"- Governance State: {dashboard['governance_conditions']['governance_state']}",
        f"- Governance Action: {dashboard['governance_conditions']['governance_action']}",
        f"- Recursion Mode: {dashboard['governance_conditions']['recursion_mode']}",
        "",

        "## Timeline",
        "",

        f"- Transition State: {dashboard['timeline_conditions']['latest_transition_state']}",
        f"- Recursive Direction: {dashboard['timeline_conditions']['latest_recursive_direction']}",
        f"- Regime Shift Events: {dashboard['timeline_conditions']['regime_shift_events']}",
        "",
    ]

    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(md_lines))

    summary = {

        "component": "offline_executive_dashboard_v1",

        "generated_at_utc": datetime.now(UTC).isoformat(),

        "runtime_health": dashboard["executive_header"]["runtime_health"],

        "systemic_risk_level": dashboard["executive_header"]["systemic_risk_level"],

        "global_recursive_mode": dashboard["executive_header"]["global_recursive_mode"],

        "highest_risk_branch": dashboard["scenario_conditions"]["highest_risk_branch"],

        "timeline_state": dashboard["timeline_conditions"]["latest_transition_state"],

        "governance_state": dashboard["governance_conditions"]["governance_state"],

        "dashboard_state": "offline_dashboard_operational",

        "status": "offline_executive_dashboard_complete",
    }

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(
            summary,
            f,
            indent=2,
        )

    print("Offline executive dashboard complete")
    print("Runtime Health:", summary["runtime_health"])
    print("Systemic Risk Level:", summary["systemic_risk_level"])
    print("Global Recursive Mode:", summary["global_recursive_mode"])
    print("Highest Risk Branch:", summary["highest_risk_branch"])
    print("Timeline State:", summary["timeline_state"])
    print("JSON:", json_path)
    print("MARKDOWN:", md_path)
    print("SUMMARY:", summary_path)
    print("Summary:", summary)

    return dashboard


if __name__ == "__main__":
    build_offline_executive_dashboard_v1()

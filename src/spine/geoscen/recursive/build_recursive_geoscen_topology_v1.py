from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


def classify_topology_state(score):
    if score >= 0.85:
        return "recursive_cascade_topology"

    if score >= 0.70:
        return "systemic_recursive_topology"

    if score >= 0.55:
        return "fragile_recursive_topology"

    if score >= 0.40:
        return "elevated_recursive_topology"

    if score >= 0.25:
        return "watch_recursive_topology"

    return "stable_recursive_topology"


def build_recursive_geoscen_topology_v1():
    repo_root = Path.cwd()

    recursive_dir = (
        repo_root
        / "data"
        / "geoscen"
        / "recursive"
    )

    registry_path = recursive_dir / "geoscen_systemic_escalation_registry_summary_v1.json"
    recursive_path = recursive_dir / "recursive_escalation_engine_summary_v1.json"
    fragility_path = recursive_dir / "systemic_fragility_state_machine_summary_v1.json"
    memory_path = recursive_dir / "recursive_topology_memory_summary_v1.json"
    contagion_path = recursive_dir / "recursive_contagion_propagation_summary_v1.json"
    governance_path = recursive_dir / "geoscen_recursive_governance_summary_v1.json"

    with open(registry_path, "r", encoding="utf-8") as f:
        registry = json.load(f)

    with open(recursive_path, "r", encoding="utf-8") as f:
        recursive = json.load(f)

    with open(fragility_path, "r", encoding="utf-8") as f:
        fragility = json.load(f)

    with open(memory_path, "r", encoding="utf-8") as f:
        memory = json.load(f)

    with open(contagion_path, "r", encoding="utf-8") as f:
        contagion = json.load(f)

    with open(governance_path, "r", encoding="utf-8") as f:
        governance = json.load(f)

    systemic_escalation_score = float(
        registry.get("systemic_escalation_score", 0.0) or 0.0
    )

    recursive_escalation_pressure = float(
        recursive.get("recursive_escalation_pressure", 0.0) or 0.0
    )

    systemic_fragility_score = float(
        fragility.get("systemic_fragility_score", 0.0) or 0.0
    )

    cascade_probability = float(
        fragility.get("cascade_probability", 0.0) or 0.0
    )

    memory_score = float(
        memory.get("memory_score", 0.0) or 0.0
    )

    avg_recursive_contagion_pressure = float(
        contagion.get("avg_recursive_contagion_pressure", 0.0) or 0.0
    )

    max_recursive_contagion_pressure = float(
        contagion.get("max_recursive_contagion_pressure", 0.0) or 0.0
    )

    governance_pressure = float(
        governance.get("governance_pressure", 0.0) or 0.0
    )

    governance_clear = (
        governance.get("governance_state") == "governance_clear"
        and governance.get("recursion_allowed") is True
        and governance.get("amplification_allowed") is True
    )

    raw_topology_score = (
        0.18 * systemic_escalation_score
        + 0.12 * recursive_escalation_pressure
        + 0.20 * systemic_fragility_score
        + 0.14 * cascade_probability
        + 0.10 * memory_score
        + 0.16 * avg_recursive_contagion_pressure
        + 0.10 * max_recursive_contagion_pressure
    )

    if governance_clear:
        governance_adjustment = 1.00
    elif governance.get("governance_state") == "governance_watch":
        governance_adjustment = 0.90
    elif governance.get("governance_state") == "governance_throttle":
        governance_adjustment = 0.75
    else:
        governance_adjustment = 0.50

    recursive_topology_score = round(
        min(
            1.0,
            raw_topology_score * governance_adjustment,
        ),
        4,
    )

    topology_state = classify_topology_state(recursive_topology_score)

    topology_nodes = [
        {
            "node": "systemic_registry",
            "score": round(systemic_escalation_score, 4),
            "role": "global_escalation_memory",
        },
        {
            "node": "recursive_escalation",
            "score": round(recursive_escalation_pressure, 4),
            "role": "cross_engine_feedback",
        },
        {
            "node": "fragility_state_machine",
            "score": round(systemic_fragility_score, 4),
            "role": "macro_fragility_state",
        },
        {
            "node": "topology_memory",
            "score": round(memory_score, 4),
            "role": "path_dependency",
        },
        {
            "node": "recursive_contagion",
            "score": round(avg_recursive_contagion_pressure, 4),
            "role": "propagation_pressure",
        },
        {
            "node": "governance",
            "score": round(governance_pressure, 4),
            "role": "recursion_control",
        },
    ]

    topology_edges = [
        {
            "source": "systemic_registry",
            "target": "recursive_escalation",
            "edge_type": "state_to_feedback",
        },
        {
            "source": "recursive_escalation",
            "target": "fragility_state_machine",
            "edge_type": "feedback_to_fragility",
        },
        {
            "source": "fragility_state_machine",
            "target": "topology_memory",
            "edge_type": "fragility_to_memory",
        },
        {
            "source": "topology_memory",
            "target": "recursive_contagion",
            "edge_type": "memory_to_propagation",
        },
        {
            "source": "recursive_contagion",
            "target": "governance",
            "edge_type": "propagation_to_governance",
        },
        {
            "source": "governance",
            "target": "systemic_registry",
            "edge_type": "governance_to_state_control",
        },
    ]

    topology_df = pd.DataFrame(topology_nodes)
    edge_df = pd.DataFrame(topology_edges)

    summary = {
        "component": "recursive_geoscen_topology_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "recursive_topology_score": recursive_topology_score,
        "recursive_topology_state": topology_state,
        "governance_adjustment": governance_adjustment,
        "governance_state": governance.get("governance_state"),
        "recursion_mode": governance.get("recursion_mode"),
        "node_count": len(topology_nodes),
        "edge_count": len(topology_edges),
        "topology_nodes": topology_nodes,
        "topology_edges": topology_edges,
        "interpretation": {
            "current_state": topology_state,
            "recursive_contagion_state": contagion.get("recursive_contagion_state"),
            "fragility_state": fragility.get("systemic_fragility_state"),
            "memory_state": memory.get("memory_state"),
            "governance_action": governance.get("governance_action"),
        },
        "status": "recursive_geoscen_topology_complete",
    }

    node_parquet = recursive_dir / "recursive_geoscen_topology_nodes_v1.parquet"
    edge_parquet = recursive_dir / "recursive_geoscen_topology_edges_v1.parquet"
    node_json = recursive_dir / "recursive_geoscen_topology_nodes_v1.json"
    edge_json = recursive_dir / "recursive_geoscen_topology_edges_v1.json"
    summary_path = recursive_dir / "recursive_geoscen_topology_summary_v1.json"

    topology_df.to_parquet(node_parquet, index=False)
    edge_df.to_parquet(edge_parquet, index=False)

    topology_df.to_json(
        node_json,
        orient="records",
        indent=2,
    )

    edge_df.to_json(
        edge_json,
        orient="records",
        indent=2,
    )

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Recursive GeoScen topology complete")
    print("Recursive Topology Score:", recursive_topology_score)
    print("Recursive Topology State:", topology_state)
    print("Governance Adjustment:", governance_adjustment)
    print("NODE PARQUET:", node_parquet)
    print("EDGE PARQUET:", edge_parquet)
    print("NODE JSON:", node_json)
    print("EDGE JSON:", edge_json)
    print("SUMMARY:", summary_path)
    print("Summary:", summary)

    return summary


if __name__ == "__main__":
    build_recursive_geoscen_topology_v1()

from pathlib import Path
from datetime import datetime, UTC
import json


RECURSION_CAP = 0.70
CASCADE_CAP = 0.65
AMPLIFICATION_CAP = 0.30
MEMORY_CAP = 0.60


def governance_state(score):
    if score >= 0.80:
        return "governance_lockdown"

    if score >= 0.65:
        return "governance_throttle"

    if score >= 0.45:
        return "governance_watch"

    return "governance_clear"


def build_geoscen_recursive_governance_v1():
    repo_root = Path.cwd()

    recursive_dir = (
        repo_root
        / "data"
        / "geoscen"
        / "recursive"
    )

    fragility_path = recursive_dir / "systemic_fragility_state_machine_summary_v1.json"
    contagion_path = recursive_dir / "recursive_contagion_propagation_summary_v1.json"
    recursive_path = recursive_dir / "recursive_escalation_engine_summary_v1.json"
    memory_path = recursive_dir / "recursive_topology_memory_summary_v1.json"

    with open(fragility_path, "r", encoding="utf-8") as f:
        fragility = json.load(f)

    with open(contagion_path, "r", encoding="utf-8") as f:
        contagion = json.load(f)

    with open(recursive_path, "r", encoding="utf-8") as f:
        recursive = json.load(f)

    with open(memory_path, "r", encoding="utf-8") as f:
        memory = json.load(f)

    systemic_fragility_score = float(
        fragility.get("systemic_fragility_score", 0.0) or 0.0
    )

    cascade_probability = float(
        fragility.get("cascade_probability", 0.0) or 0.0
    )

    recursive_escalation_pressure = float(
        recursive.get("recursive_escalation_pressure", 0.0) or 0.0
    )

    max_recursive_feedback = float(
        recursive.get("max_recursive_feedback", 0.0) or 0.0
    )

    avg_recursive_contagion_pressure = float(
        contagion.get("avg_recursive_contagion_pressure", 0.0) or 0.0
    )

    max_recursive_contagion_pressure = float(
        contagion.get("max_recursive_contagion_pressure", 0.0) or 0.0
    )

    memory_score = float(
        memory.get("memory_score", 0.0) or 0.0
    )

    raw_governance_pressure = (
        0.25 * systemic_fragility_score
        + 0.20 * cascade_probability
        + 0.15 * recursive_escalation_pressure
        + 0.15 * avg_recursive_contagion_pressure
        + 0.15 * max_recursive_contagion_pressure
        + 0.10 * memory_score
    )

    governance_pressure = round(
        min(1.0, raw_governance_pressure),
        4,
    )

    amplification_allowed = (
        recursive_escalation_pressure <= AMPLIFICATION_CAP
        and max_recursive_feedback <= AMPLIFICATION_CAP
        and avg_recursive_contagion_pressure <= AMPLIFICATION_CAP
    )

    recursion_allowed = (
        systemic_fragility_score <= RECURSION_CAP
        and cascade_probability <= CASCADE_CAP
        and memory_score <= MEMORY_CAP
    )

    throttle_required = (
        governance_pressure >= 0.45
        or not amplification_allowed
        or not recursion_allowed
    )

    lockdown_required = (
        governance_pressure >= 0.80
        or systemic_fragility_score >= 0.85
        or cascade_probability >= 0.85
    )

    if lockdown_required:
        governance_action = "lockdown_recursive_amplification"
        recursion_mode = "halt_recursive_feedback"
    elif throttle_required:
        governance_action = "throttle_recursive_amplification"
        recursion_mode = "limited_recursive_feedback"
    else:
        governance_action = "allow_recursive_amplification"
        recursion_mode = "normal_recursive_feedback"

    output = {
        "component": "geoscen_recursive_governance_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "inputs": {
            "systemic_fragility_score": round(systemic_fragility_score, 4),
            "cascade_probability": round(cascade_probability, 4),
            "recursive_escalation_pressure": round(recursive_escalation_pressure, 4),
            "max_recursive_feedback": round(max_recursive_feedback, 4),
            "avg_recursive_contagion_pressure": round(avg_recursive_contagion_pressure, 4),
            "max_recursive_contagion_pressure": round(max_recursive_contagion_pressure, 4),
            "memory_score": round(memory_score, 4),
        },
        "caps": {
            "recursion_cap": RECURSION_CAP,
            "cascade_cap": CASCADE_CAP,
            "amplification_cap": AMPLIFICATION_CAP,
            "memory_cap": MEMORY_CAP,
        },
        "governance_pressure": governance_pressure,
        "governance_state": governance_state(governance_pressure),
        "amplification_allowed": amplification_allowed,
        "recursion_allowed": recursion_allowed,
        "throttle_required": throttle_required,
        "lockdown_required": lockdown_required,
        "governance_action": governance_action,
        "recursion_mode": recursion_mode,
        "status": "recursive_governance_complete",
    }

    summary_path = recursive_dir / "geoscen_recursive_governance_summary_v1.json"
    json_path = recursive_dir / "geoscen_recursive_governance_v1.json"

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    print("GeoScen recursive governance complete")
    print("Governance Pressure:", governance_pressure)
    print("Governance State:", output["governance_state"])
    print("Governance Action:", governance_action)
    print("Recursion Mode:", recursion_mode)
    print("SUMMARY:", summary_path)
    print("JSON:", json_path)
    print("Summary:", output)

    return output


if __name__ == "__main__":
    build_geoscen_recursive_governance_v1()

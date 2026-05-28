from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


SCENARIOS = [
    {
        "scenario": "fade",
        "description": "Recursive pressures fade as cross-domain stress normalizes.",
        "pressure_multiplier": 0.70,
        "persistence_multiplier": 0.60,
    },
    {
        "scenario": "base",
        "description": "Recursive pressures persist near current levels.",
        "pressure_multiplier": 1.00,
        "persistence_multiplier": 1.00,
    },
    {
        "scenario": "acceleration",
        "description": "Recursive pressures accelerate across domains.",
        "pressure_multiplier": 1.30,
        "persistence_multiplier": 1.20,
    },
    {
        "scenario": "cascade",
        "description": "Recursive feedback loops amplify into broader systemic stress.",
        "pressure_multiplier": 1.60,
        "persistence_multiplier": 1.40,
    },
]


def classify_projection_state(score):
    if score >= 0.80:
        return "projected_cascade_risk"

    if score >= 0.65:
        return "projected_systemic_fragility"

    if score >= 0.50:
        return "projected_elevated_fragility"

    if score >= 0.35:
        return "projected_watch"

    return "projected_stable"


def build_recursive_scenario_projection_v1():
    repo_root = Path.cwd()

    fusion_dir = repo_root / "data" / "geoscen" / "fusion"
    recursive_dir = repo_root / "data" / "geoscen" / "recursive"
    out_dir = repo_root / "data" / "geoscen" / "projection"

    out_dir.mkdir(parents=True, exist_ok=True)

    fusion_summary_path = fusion_dir / "cross_domain_recursive_fusion_summary_v1.json"
    regime_memory_path = fusion_dir / "recursive_regime_memory_summary_v1.json"
    fragility_path = recursive_dir / "systemic_fragility_state_machine_summary_v1.json"
    topology_path = recursive_dir / "recursive_geoscen_topology_summary_v1.json"
    governance_path = recursive_dir / "geoscen_recursive_governance_summary_v1.json"

    with open(fusion_summary_path, "r", encoding="utf-8") as f:
        fusion = json.load(f)

    with open(regime_memory_path, "r", encoding="utf-8") as f:
        memory = json.load(f)

    with open(fragility_path, "r", encoding="utf-8") as f:
        fragility = json.load(f)

    with open(topology_path, "r", encoding="utf-8") as f:
        topology = json.load(f)

    with open(governance_path, "r", encoding="utf-8") as f:
        governance = json.load(f)

    cross_domain_recursive_pressure = float(
        fusion.get("cross_domain_recursive_pressure", 0.0) or 0.0
    )

    avg_adjusted_target_pressure = float(
        fusion.get("avg_adjusted_target_pressure", 0.0) or 0.0
    )

    max_adjusted_target_pressure = float(
        fusion.get("max_adjusted_target_pressure", 0.0) or 0.0
    )

    regime_memory_score = float(
        memory.get("regime_memory_score", 0.0) or 0.0
    )

    current_regime_memory_input = float(
        memory.get("current_regime_memory_input", 0.0) or 0.0
    )

    systemic_fragility_score = float(
        fragility.get("systemic_fragility_score", 0.0) or 0.0
    )

    cascade_probability = float(
        fragility.get("cascade_probability", 0.0) or 0.0
    )

    recursive_topology_score = float(
        topology.get("recursive_topology_score", 0.0) or 0.0
    )

    governance_pressure = float(
        governance.get("governance_pressure", 0.0) or 0.0
    )

    governance_state = governance.get("governance_state", "unknown")

    governance_penalty = 1.00

    if governance_state == "governance_watch":
        governance_penalty = 0.95
    elif governance_state == "governance_throttle":
        governance_penalty = 0.80
    elif governance_state == "governance_lockdown":
        governance_penalty = 0.60

    base_projection_pressure = (
        0.22 * cross_domain_recursive_pressure
        + 0.18 * avg_adjusted_target_pressure
        + 0.16 * max_adjusted_target_pressure
        + 0.16 * systemic_fragility_score
        + 0.12 * cascade_probability
        + 0.08 * recursive_topology_score
        + 0.08 * current_regime_memory_input
    )

    rows = []

    for item in SCENARIOS:
        projected_pressure = min(
            1.0,
            base_projection_pressure
            * item["pressure_multiplier"]
            * governance_penalty,
        )

        projected_memory_pressure = min(
            1.0,
            (
                0.65 * regime_memory_score
                + 0.35 * current_regime_memory_input
            )
            * item["persistence_multiplier"],
        )

        projected_systemic_score = min(
            1.0,
            0.65 * projected_pressure
            + 0.35 * projected_memory_pressure,
        )

        projected_cascade_probability = min(
            1.0,
            0.50 * projected_systemic_score
            + 0.30 * cascade_probability * item["pressure_multiplier"]
            + 0.20 * max_adjusted_target_pressure,
        )

        rows.append(
            {
                "scenario": item["scenario"],
                "description": item["description"],
                "pressure_multiplier": item["pressure_multiplier"],
                "persistence_multiplier": item["persistence_multiplier"],
                "projected_pressure": round(projected_pressure, 4),
                "projected_memory_pressure": round(projected_memory_pressure, 4),
                "projected_systemic_score": round(projected_systemic_score, 4),
                "projected_cascade_probability": round(projected_cascade_probability, 4),
                "projected_state": classify_projection_state(projected_systemic_score),
                "governance_state": governance_state,
                "governance_penalty": governance_penalty,
            }
        )

    df = pd.DataFrame(rows)

    summary = {
        "component": "recursive_scenario_projection_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "scenario_count": int(len(df)),
        "base_projection_pressure": round(base_projection_pressure, 4),
        "current_regime_memory_score": round(regime_memory_score, 4),
        "current_regime_memory_input": round(current_regime_memory_input, 4),
        "current_fragility_score": round(systemic_fragility_score, 4),
        "current_cascade_probability": round(cascade_probability, 4),
        "governance_state": governance_state,
        "governance_penalty": governance_penalty,
        "highest_projected_state": df.sort_values(
            "projected_systemic_score",
            ascending=False,
        ).iloc[0]["projected_state"],
        "highest_projected_scenario": df.sort_values(
            "projected_systemic_score",
            ascending=False,
        ).iloc[0]["scenario"],
        "max_projected_systemic_score": round(
            float(df["projected_systemic_score"].max()),
            4,
        ),
        "max_projected_cascade_probability": round(
            float(df["projected_cascade_probability"].max()),
            4,
        ),
        "status": "recursive_scenario_projection_complete",
    }

    parquet_path = out_dir / "recursive_scenario_projection_v1.parquet"
    json_path = out_dir / "recursive_scenario_projection_v1.json"
    summary_path = out_dir / "recursive_scenario_projection_summary_v1.json"

    df.to_parquet(parquet_path, index=False)

    df.to_json(
        json_path,
        orient="records",
        indent=2,
    )

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Recursive scenario projection complete")
    print("Rows:", len(df))
    print("Base Projection Pressure:", round(base_projection_pressure, 4))
    print("Highest Scenario:", summary["highest_projected_scenario"])
    print("Highest State:", summary["highest_projected_state"])
    print("Max Projected Systemic Score:", summary["max_projected_systemic_score"])
    print("Max Projected Cascade Probability:", summary["max_projected_cascade_probability"])
    print("PARQUET:", parquet_path)
    print("JSON:", json_path)
    print("SUMMARY:", summary_path)
    print("Summary:", summary)

    return df


if __name__ == "__main__":
    build_recursive_scenario_projection_v1()

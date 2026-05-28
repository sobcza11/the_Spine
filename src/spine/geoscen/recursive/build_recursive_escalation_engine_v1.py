from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


FEEDBACK_LINKS = [
    {
        "source_engine": "COT",
        "target_engine": "COT_IV",
        "feedback_type": "positioning_to_transition",
        "amplification_weight": 0.30,
    },
    {
        "source_engine": "COT_IV",
        "target_engine": "COT_CONTAGION",
        "feedback_type": "transition_to_contagion",
        "amplification_weight": 0.25,
    },
    {
        "source_engine": "COT_CONTAGION",
        "target_engine": "COT_REGIME",
        "feedback_type": "contagion_to_regime",
        "amplification_weight": 0.25,
    },
    {
        "source_engine": "COT_REGIME",
        "target_engine": "COT",
        "feedback_type": "regime_to_positioning",
        "amplification_weight": 0.20,
    },
    {
        "source_engine": "COT_VALIDATION",
        "target_engine": "COT",
        "feedback_type": "validation_to_confidence",
        "amplification_weight": 0.10,
    },
]


def classify_recursive_state(score):
    if score >= 0.75:
        return "recursive_systemic"

    if score >= 0.60:
        return "recursive_fragile"

    if score >= 0.40:
        return "recursive_elevated"

    if score >= 0.25:
        return "recursive_watch"

    return "recursive_stable"


def build_recursive_escalation_engine_v1():
    repo_root = Path.cwd()

    registry_path = (
        repo_root
        / "data"
        / "geoscen"
        / "recursive"
        / "geoscen_systemic_escalation_registry_v1.parquet"
    )

    out_dir = (
        repo_root
        / "data"
        / "geoscen"
        / "recursive"
    )

    out_dir.mkdir(parents=True, exist_ok=True)

    registry = pd.read_parquet(registry_path).copy()

    required_cols = {
        "engine",
        "engine_score",
        "engine_max_score",
        "engine_weight",
        "engine_confidence",
        "weighted_escalation",
        "escalation_state",
    }

    missing = required_cols - set(registry.columns)
    if missing:
        raise KeyError(f"Missing required registry columns: {missing}")

    score_map = dict(
        zip(
            registry["engine"],
            registry["engine_score"],
        )
    )

    confidence_map = dict(
        zip(
            registry["engine"],
            registry["engine_confidence"],
        )
    )

    state_map = dict(
        zip(
            registry["engine"],
            registry["escalation_state"],
        )
    )

    rows = []

    for link in FEEDBACK_LINKS:
        source = link["source_engine"]
        target = link["target_engine"]

        source_score = float(score_map.get(source, 0.0))
        target_score = float(score_map.get(target, 0.0))

        source_confidence = float(confidence_map.get(source, 0.0))
        target_confidence = float(confidence_map.get(target, 0.0))

        amplification_weight = float(link["amplification_weight"])

        recursive_feedback_pressure = (
            source_score
            * target_score
            * amplification_weight
            * min(source_confidence, target_confidence)
        )

        recursive_feedback_pressure = round(
            min(1.0, recursive_feedback_pressure),
            4,
        )

        adjusted_target_pressure = round(
            min(
                1.0,
                target_score + recursive_feedback_pressure,
            ),
            4,
        )

        rows.append(
            {
                "source_engine": source,
                "target_engine": target,
                "feedback_type": link["feedback_type"],
                "source_score": round(source_score, 4),
                "target_score": round(target_score, 4),
                "source_state": state_map.get(source, "unknown"),
                "target_state": state_map.get(target, "unknown"),
                "source_confidence": round(source_confidence, 4),
                "target_confidence": round(target_confidence, 4),
                "amplification_weight": amplification_weight,
                "recursive_feedback_pressure": recursive_feedback_pressure,
                "adjusted_target_pressure": adjusted_target_pressure,
                "recursive_state": classify_recursive_state(
                    adjusted_target_pressure
                ),
            }
        )

    feedback = pd.DataFrame(rows)

    recursive_escalation_pressure = round(
        float(feedback["recursive_feedback_pressure"].mean()),
        4,
    )

    max_recursive_feedback = round(
        float(feedback["recursive_feedback_pressure"].max()),
        4,
    )

    avg_adjusted_target_pressure = round(
        float(feedback["adjusted_target_pressure"].mean()),
        4,
    )

    max_adjusted_target_pressure = round(
        float(feedback["adjusted_target_pressure"].max()),
        4,
    )

    recursive_systemic_state = classify_recursive_state(
        avg_adjusted_target_pressure
    )

    summary = {
        "component": "recursive_escalation_engine_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "feedback_link_count": int(len(feedback)),
        "recursive_escalation_pressure": recursive_escalation_pressure,
        "max_recursive_feedback": max_recursive_feedback,
        "avg_adjusted_target_pressure": avg_adjusted_target_pressure,
        "max_adjusted_target_pressure": max_adjusted_target_pressure,
        "recursive_systemic_state": recursive_systemic_state,
        "feedback_types": feedback["feedback_type"].tolist(),
        "highest_pressure_links": feedback.sort_values(
            "recursive_feedback_pressure",
            ascending=False,
        )
        .head(3)[
            [
                "source_engine",
                "target_engine",
                "recursive_feedback_pressure",
            ]
        ]
        .to_dict(orient="records"),
        "status": "recursive_escalation_engine_complete",
    }

    parquet_path = out_dir / "recursive_escalation_engine_v1.parquet"
    json_path = out_dir / "recursive_escalation_engine_v1.json"
    summary_path = out_dir / "recursive_escalation_engine_summary_v1.json"

    feedback.to_parquet(parquet_path, index=False)

    feedback.to_json(
        json_path,
        orient="records",
        indent=2,
    )

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Recursive escalation engine complete")
    print("Rows:", len(feedback))
    print("Recursive Escalation Pressure:", recursive_escalation_pressure)
    print("Recursive Systemic State:", recursive_systemic_state)
    print("PARQUET:", parquet_path)
    print("JSON:", json_path)
    print("SUMMARY:", summary_path)
    print("Summary:", summary)

    return feedback


if __name__ == "__main__":
    build_recursive_escalation_engine_v1()

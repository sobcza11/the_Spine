from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


ENGINE_CONFIG = {
    "COT": {
        "source_file": ["data", "geoscen", "conditioning", "geoscen_conditioned_cot_routing_summary_v1.json"],
        "score_key": "avg_conditioned_cot_stress",
        "max_key": "max_conditioned_cot_stress",
        "default_weight": 0.25,
        "confidence": 0.95,
    },
    "COT_IV": {
        "source_file": ["data", "cot", "routing", "cot_iv_vector_summary_v1.json"],
        "score_key": "cot_iv_transition_pressure",
        "max_key": "max_transition_pressure",
        "default_weight": 0.20,
        "confidence": 0.90,
    },
    "COT_CONTAGION": {
        "source_file": ["data", "cot", "routing", "cot_cross_asset_contagion_summary_v1.json"],
        "score_key": "avg_contagion_pressure",
        "max_key": "max_contagion_pressure",
        "default_weight": 0.20,
        "confidence": 0.88,
    },
    "COT_REGIME": {
        "source_file": ["data", "cot", "routing", "cot_regime_conditioned_behavior_summary_v1.json"],
        "score_key": "avg_latest_instability",
        "max_key": "max_latest_instability",
        "default_weight": 0.20,
        "confidence": 0.90,
    },
    "COT_VALIDATION": {
        "source_file": ["data", "cot", "validation", "cot_historical_stress_validation_summary_v1.json"],
        "score_key": "avg_validation_score",
        "max_key": "max_validation_score",
        "default_weight": 0.15,
        "confidence": 0.75,
    },
}


def classify_escalation(score):
    if score >= 0.75:
        return "systemic"

    if score >= 0.60:
        return "fragile"

    if score >= 0.40:
        return "elevated"

    if score >= 0.25:
        return "watch"

    return "stable"


def load_summary(repo_root, config):
    path = repo_root.joinpath(*config["source_file"])

    if not path.exists():
        return None, path

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f), path


def build_geoscen_systemic_escalation_registry_v1():
    repo_root = Path.cwd()

    out_dir = repo_root / "data" / "geoscen" / "recursive"
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = []

    for engine_name, config in ENGINE_CONFIG.items():
        summary, path = load_summary(repo_root, config)

        if summary is None:
            rows.append(
                {
                    "engine": engine_name,
                    "source_path": str(path),
                    "source_available": False,
                    "engine_score": 0.0,
                    "engine_max_score": 0.0,
                    "engine_weight": config["default_weight"],
                    "engine_confidence": 0.0,
                    "weighted_escalation": 0.0,
                    "escalation_state": "missing",
                }
            )
            continue

        score = float(summary.get(config["score_key"], 0.0) or 0.0)
        max_score = float(summary.get(config["max_key"], score) or score)
        weight = float(config["default_weight"])
        confidence = float(config["confidence"])

        weighted_escalation = round(score * weight * confidence, 4)

        rows.append(
            {
                "engine": engine_name,
                "source_path": str(path),
                "source_available": True,
                "engine_score": round(score, 4),
                "engine_max_score": round(max_score, 4),
                "engine_weight": weight,
                "engine_confidence": confidence,
                "weighted_escalation": weighted_escalation,
                "escalation_state": classify_escalation(score),
            }
        )

    registry = pd.DataFrame(rows)

    total_weighted_escalation = round(
        float(registry["weighted_escalation"].sum()),
        4,
    )

    max_engine_score = round(
        float(registry["engine_max_score"].max()),
        4,
    )

    systemic_escalation_score = min(
        1.0,
        round(total_weighted_escalation, 4),
    )

    systemic_state = classify_escalation(systemic_escalation_score)

    summary = {
        "component": "geoscen_systemic_escalation_registry_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "engine_count": int(len(registry)),
        "active_engine_count": int(registry["source_available"].sum()),
        "systemic_escalation_score": systemic_escalation_score,
        "max_engine_score": max_engine_score,
        "systemic_state": systemic_state,
        "active_engines": registry[registry["source_available"]]["engine"].tolist(),
        "missing_engines": registry[~registry["source_available"]]["engine"].tolist(),
        "engine_states": dict(zip(registry["engine"], registry["escalation_state"])),
        "status": "systemic_escalation_registry_complete",
    }

    parquet_path = out_dir / "geoscen_systemic_escalation_registry_v1.parquet"
    json_path = out_dir / "geoscen_systemic_escalation_registry_v1.json"
    summary_path = out_dir / "geoscen_systemic_escalation_registry_summary_v1.json"

    registry.to_parquet(parquet_path, index=False)

    registry.to_json(
        json_path,
        orient="records",
        indent=2,
    )

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("GeoScen systemic escalation registry complete")
    print("Rows:", len(registry))
    print("Systemic Score:", systemic_escalation_score)
    print("Systemic State:", systemic_state)
    print("PARQUET:", parquet_path)
    print("JSON:", json_path)
    print("SUMMARY:", summary_path)
    print("Summary:", summary)

    return registry


if __name__ == "__main__":
    build_geoscen_systemic_escalation_registry_v1()

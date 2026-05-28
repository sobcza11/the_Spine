from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


RUN_TS = datetime.now(timezone.utc).isoformat()

INPUT_JSON = Path(
    "data/serving/geoscen/geoscen_final_metric_engine_v1.json"
)

OUTPUT_JSON = Path(
    "data/serving/geoscen/geoscen_historical_regime_memory_v1.json"
)

OUTPUT_PARQUET = Path(
    "data/serving/geoscen/geoscen_historical_regime_memory_v1.parquet"
)


HISTORICAL_REGIMES = [
    {
        "name": "2008 Liquidity Stress",
        "regime_type": "liquidity_stress",
        "macro_temperature": "COLD",
        "contradiction_range": (0.30, 0.80),
        "instability_range": (0.60, 1.00),
        "confidence_floor": 0.40,
        "description": "Systemic liquidity deterioration & cross-asset stress.",
    },
    {
        "name": "1970s Inflation Shock",
        "regime_type": "late_cycle_tightening",
        "macro_temperature": "HOT",
        "contradiction_range": (0.15, 0.60),
        "instability_range": (0.35, 0.80),
        "confidence_floor": 0.50,
        "description": "Persistent inflationary pressure & restrictive policy response.",
    },
    {
        "name": "1998 FX Contagion",
        "regime_type": "fragmented_cross_asset",
        "macro_temperature": "NEUTRAL",
        "contradiction_range": (0.35, 0.90),
        "instability_range": (0.45, 0.90),
        "confidence_floor": 0.35,
        "description": "Fragmented global liquidity & FX instability.",
    },
    {
        "name": "2020 Liquidity Shock",
        "regime_type": "liquidity_stress",
        "macro_temperature": "COLD",
        "contradiction_range": (0.40, 1.00),
        "instability_range": (0.70, 1.00),
        "confidence_floor": 0.30,
        "description": "Rapid global liquidity seizure & emergency stabilization.",
    },
    {
        "name": "2022 Tightening Cycle",
        "regime_type": "late_cycle_tightening",
        "macro_temperature": "WARM",
        "contradiction_range": (0.20, 0.70),
        "instability_range": (0.40, 0.85),
        "confidence_floor": 0.55,
        "description": "Aggressive policy normalization & duration stress.",
    },
]


def load_metric_payload() -> Dict[str, Any]:
    if not INPUT_JSON.exists():
        raise FileNotFoundError(f"Missing required input: {INPUT_JSON}")

    with INPUT_JSON.open("r", encoding="utf-8") as f:
        return json.load(f)


def within_range(value: float, bounds: tuple[float, float]) -> bool:
    return bounds[0] <= value <= bounds[1]


def compute_similarity_score(
    current_regime_type: str,
    current_temperature: str,
    contradiction_score: float,
    instability_risk: float,
    confidence: float,
    historical: Dict[str, Any],
) -> float:

    score = 0.0

    if current_regime_type == historical["regime_type"]:
        score += 0.35

    if current_temperature == historical["macro_temperature"]:
        score += 0.20

    if within_range(
        contradiction_score,
        historical["contradiction_range"],
    ):
        score += 0.20

    if within_range(
        instability_risk,
        historical["instability_range"],
    ):
        score += 0.15

    if confidence >= historical["confidence_floor"]:
        score += 0.10

    return round(min(score, 1.0), 4)


def build_historical_matches(
    metric_payload: Dict[str, Any]
) -> List[Dict[str, Any]]:

    metrics = metric_payload["metrics"]
    regime = metric_payload["regime"]

    current_regime_type = regime["regime_key"]
    current_temperature = metrics["macro_temperature"]

    contradiction_score = metrics["contradiction_score"]
    instability_risk = metrics["instability_risk"]
    confidence = metrics["confidence"]

    matches: List[Dict[str, Any]] = []

    for historical in HISTORICAL_REGIMES:

        similarity = compute_similarity_score(
            current_regime_type=current_regime_type,
            current_temperature=current_temperature,
            contradiction_score=contradiction_score,
            instability_risk=instability_risk,
            confidence=confidence,
            historical=historical,
        )

        matches.append(
            {
                "historical_regime": historical["name"],
                "regime_type": historical["regime_type"],
                "similarity_score": similarity,
                "description": historical["description"],
            }
        )

    matches = sorted(
        matches,
        key=lambda x: x["similarity_score"],
        reverse=True,
    )

    return matches


def build_memory_payload(
    metric_payload: Dict[str, Any]
) -> Dict[str, Any]:

    matches = build_historical_matches(
        metric_payload
    )

    top_match = matches[0] if matches else {}

    deployment_ready = bool(
        metric_payload.get("deployment_ready", False)
        and top_match.get("similarity_score", 0.0) >= 0.50
    )

    return {
        "artifact": "geoscen_historical_regime_memory_v1",
        "system": "GeoScen",
        "layer": "Historical Regime Memory",
        "version": "v1",
        "run_ts": RUN_TS,
        "deployment_ready": deployment_ready,
        "current_regime": metric_payload["regime"],
        "current_metrics": metric_payload["metrics"],
        "historical_matches": matches,
        "top_match": top_match,
        "routing": {
            "oraclechambers_ready": True,
            "historical_cognition_layer": True,
            "regime_memory_enabled": True,
            "ai_dependency": False,
        },
        "provenance": {
            "source_payload": str(INPUT_JSON),
            "source_artifact": metric_payload["artifact"],
            "source_run_ts": metric_payload["run_ts"],
        },
    }


def write_outputs(payload: Dict[str, Any]) -> None:
    OUTPUT_JSON.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with OUTPUT_JSON.open(
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(
            payload,
            f,
            indent=2,
            ensure_ascii=False,
        )

    top_match = payload.get("top_match", {})

    parquet_row = {
        "artifact": payload["artifact"],
        "system": payload["system"],
        "layer": payload["layer"],
        "version": payload["version"],
        "run_ts": payload["run_ts"],
        "deployment_ready": payload["deployment_ready"],
        "top_historical_match": top_match.get(
            "historical_regime"
        ),
        "similarity_score": top_match.get(
            "similarity_score"
        ),
        "historical_regime_type": top_match.get(
            "regime_type"
        ),
    }

    pd.DataFrame([parquet_row]).to_parquet(
        OUTPUT_PARQUET,
        index=False,
    )


def main() -> None:
    metric_payload = load_metric_payload()

    memory_payload = build_memory_payload(
        metric_payload
    )

    write_outputs(memory_payload)

    top_match = memory_payload.get(
        "top_match",
        {},
    )

    print(
        "GeoScen Historical Regime Memory v1 complete"
    )

    print(
        f"deployment_ready: {memory_payload['deployment_ready']}"
    )

    print(
        f"top_match: {top_match.get('historical_regime')}"
    )

    print(
        f"similarity_score: {top_match.get('similarity_score')}"
    )

    print(f"json_output: {OUTPUT_JSON}")

    print(f"parquet_output: {OUTPUT_PARQUET}")


if __name__ == "__main__":
    main()

    
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import pandas as pd


RUN_TS = datetime.now(timezone.utc).isoformat()

INPUT_JSON = Path(
    "data/serving/geoscen/geoscen_institutional_synthesis_v1.json"
)

OUTPUT_JSON = Path(
    "data/serving/oraclechambers/oc_geoscen_ingestion_bridge_v1.json"
)

OUTPUT_PARQUET = Path(
    "data/serving/oraclechambers/oc_geoscen_ingestion_bridge_v1.parquet"
)


def load_geoscen_synthesis() -> Dict[str, Any]:
    if not INPUT_JSON.exists():
        raise FileNotFoundError(f"Missing required input: {INPUT_JSON}")

    with INPUT_JSON.open("r", encoding="utf-8") as f:
        return json.load(f)


def build_panel_state(payload: Dict[str, Any]) -> Dict[str, Any]:
    regime = payload["regime"]
    metrics = payload["metrics"]
    synthesis = payload["synthesis"]
    historical = payload["historical_memory"]

    return {
        "regime_panel": {
            "title": "GeoScen Regime State",
            "regime_label": regime["regime_label"],
            "tone_direction": regime["tone_direction"],
            "confidence": metrics["confidence"],
            "conviction": metrics["conviction"],
            "macro_temperature": metrics["macro_temperature"],
        },
        "metric_panel": {
            "title": "Final Metric Layer",
            "deployability": metrics["deployability"],
            "instability_risk": metrics["instability_risk"],
            "contradiction_score": metrics["contradiction_score"],
            "drift_score": metrics["drift_score"],
            "cb_divergence_score": metrics["cb_divergence_score"],
        },
        "historical_memory_panel": {
            "title": "Historical Regime Memory",
            "top_match": historical.get("top_match", {}),
            "matches": historical.get("matches", []),
        },
        "rbl_panel": {
            "title": "Read Between the Lines",
            "risk_posture": synthesis["risk_posture"],
            "decision_bias": synthesis["decision_bias"],
            "executive_summary": synthesis["executive_summary"],
            "rbl_summary": synthesis["rbl_summary"],
        },
    }


def build_visual_routing(payload: Dict[str, Any]) -> Dict[str, Any]:
    metrics = payload["metrics"]
    regime = payload["regime"]
    synthesis = payload["synthesis"]

    instability_risk = float(metrics["instability_risk"])
    contradiction_score = float(metrics["contradiction_score"])
    drift_score = float(metrics["drift_score"])
    confidence = float(metrics["confidence"])

    priority_panel = "regime_panel"

    if instability_risk >= 0.60:
        priority_panel = "metric_panel"
    elif contradiction_score >= 0.40:
        priority_panel = "metric_panel"
    elif drift_score >= 0.40:
        priority_panel = "historical_memory_panel"
    elif confidence >= 0.80:
        priority_panel = "rbl_panel"

    return {
        "priority_panel": priority_panel,
        "show_contradiction_overlay": contradiction_score >= 0.25,
        "show_historical_overlay": bool(
            payload.get("historical_memory", {}).get("top_match")
        ),
        "show_cb_divergence_overlay": metrics["cb_divergence_score"] >= 0.20,
        "show_regime_sequence": True,
        "chart_routes": {
            "primary_chart": "regime_confidence_timeseries",
            "secondary_chart": "cross_asset_signal_map",
            "tertiary_chart": "historical_similarity_map",
        },
        "display_mode": classify_display_mode(
            regime["tone_direction"],
            synthesis["risk_posture"],
            instability_risk,
        ),
    }


def classify_display_mode(
    tone_direction: str,
    risk_posture: str,
    instability_risk: float,
) -> str:
    if instability_risk >= 0.70:
        return "defensive_monitoring"

    if risk_posture == "Elevated":
        return "risk_alert"

    if tone_direction == "mixed":
        return "fragmented_cross_asset"

    if tone_direction == "risk-on":
        return "constructive_macro"

    if tone_direction in {"risk-off", "restrictive"}:
        return "cautionary_macro"

    return "neutral_monitoring"


def build_oc_payload(
    geoscen_payload: Dict[str, Any]
) -> Dict[str, Any]:

    panel_state = build_panel_state(geoscen_payload)
    visual_routing = build_visual_routing(geoscen_payload)

    deployment_ready = bool(
        geoscen_payload.get("deployment_ready", False)
        and geoscen_payload.get("routing", {}).get("oraclechambers_ready", False)
    )

    return {
        "artifact": "oc_geoscen_ingestion_bridge_v1",
        "system": "OracleChambers",
        "source_system": "GeoScen",
        "layer": "OC GeoScen Ingestion Bridge",
        "version": "v1",
        "run_ts": RUN_TS,
        "deployment_ready": deployment_ready,
        "panel_state": panel_state,
        "visual_routing": visual_routing,
        "source_payload": {
            "artifact": geoscen_payload["artifact"],
            "run_ts": geoscen_payload["run_ts"],
            "regime": geoscen_payload["regime"],
            "metrics": geoscen_payload["metrics"],
            "synthesis": geoscen_payload["synthesis"],
        },
        "routing": {
            "oraclechambers_ready": True,
            "frontend_ready": True,
            "local_runtime_ready": True,
            "online_runtime_ready": False,
            "ai_dependency": False,
        },
        "provenance": {
            "source_payload": str(INPUT_JSON),
            "source_artifact": geoscen_payload["artifact"],
            "source_run_ts": geoscen_payload["run_ts"],
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

    panel_state = payload["panel_state"]
    visual_routing = payload["visual_routing"]

    parquet_row = {
        "artifact": payload["artifact"],
        "system": payload["system"],
        "source_system": payload["source_system"],
        "layer": payload["layer"],
        "version": payload["version"],
        "run_ts": payload["run_ts"],
        "deployment_ready": payload["deployment_ready"],
        "regime_label": panel_state["regime_panel"]["regime_label"],
        "confidence": panel_state["regime_panel"]["confidence"],
        "conviction": panel_state["regime_panel"]["conviction"],
        "macro_temperature": panel_state["regime_panel"]["macro_temperature"],
        "priority_panel": visual_routing["priority_panel"],
        "display_mode": visual_routing["display_mode"],
        "frontend_ready": payload["routing"]["frontend_ready"],
        "local_runtime_ready": payload["routing"]["local_runtime_ready"],
        "online_runtime_ready": payload["routing"]["online_runtime_ready"],
        "ai_dependency": payload["routing"]["ai_dependency"],
    }

    pd.DataFrame([parquet_row]).to_parquet(
        OUTPUT_PARQUET,
        index=False,
    )


def main() -> None:
    geoscen_payload = load_geoscen_synthesis()

    oc_payload = build_oc_payload(
        geoscen_payload
    )

    write_outputs(oc_payload)

    print("OC GeoScen Ingestion Bridge v1 complete")
    print(f"deployment_ready: {oc_payload['deployment_ready']}")
    print(f"priority_panel: {oc_payload['visual_routing']['priority_panel']}")
    print(f"display_mode: {oc_payload['visual_routing']['display_mode']}")
    print(f"json_output: {OUTPUT_JSON}")
    print(f"parquet_output: {OUTPUT_PARQUET}")


if __name__ == "__main__":
    main()
    
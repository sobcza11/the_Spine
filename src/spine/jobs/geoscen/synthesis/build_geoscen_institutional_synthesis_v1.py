from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import pandas as pd


RUN_TS = datetime.now(timezone.utc).isoformat()

INPUT_JSON = Path(
    "data/serving/geoscen/geoscen_historical_regime_memory_v1.json"
)

OUTPUT_JSON = Path(
    "data/serving/geoscen/geoscen_institutional_synthesis_v1.json"
)

OUTPUT_PARQUET = Path(
    "data/serving/geoscen/geoscen_institutional_synthesis_v1.parquet"
)


def load_memory_payload() -> Dict[str, Any]:
    if not INPUT_JSON.exists():
        raise FileNotFoundError(f"Missing required input: {INPUT_JSON}")

    with INPUT_JSON.open("r", encoding="utf-8") as f:
        return json.load(f)


def classify_risk_posture(
    instability_risk: float,
    contradiction_score: float,
) -> str:
    if instability_risk >= 0.70:
        return "Elevated"

    if contradiction_score >= 0.40:
        return "Mixed"

    if instability_risk <= 0.30:
        return "Constructive"

    return "Balanced"


def classify_decision_bias(
    tone_direction: str,
    macro_temperature: str,
    instability_risk: float,
) -> str:
    if instability_risk >= 0.70:
        return "Defensive"

    if tone_direction == "risk-on" and macro_temperature in {"WARM", "HOT"}:
        return "Constructive Risk"

    if tone_direction in {"risk-off", "restrictive"}:
        return "Caution"

    if macro_temperature == "COLD":
        return "Defensive Watch"

    return "Neutral"


def build_executive_summary(
    regime_label: str,
    confidence: float,
    conviction: float,
    instability_risk: float,
    macro_temperature: str,
    top_match: str,
) -> str:
    return (
        f"GeoScen identifies the current macro state as {regime_label}, "
        f"with confidence at {confidence:.2f}, conviction at {conviction:.2f}, "
        f"& instability risk at {instability_risk:.2f}. "
        f"Macro temperature is classified as {macro_temperature}. "
        f"The closest historical reference pattern is {top_match}."
    )


def build_rbl_summary(
    regime_label: str,
    tone_direction: str,
    contradiction_score: float,
    drift_score: float,
    cb_divergence_score: float,
) -> str:
    return (
        f"Read Between the Lines: the regime signal points to {regime_label} "
        f"with a {tone_direction} tone. Cross-compartment contradiction is "
        f"{contradiction_score:.2f}, narrative drift is {drift_score:.2f}, "
        f"& CB divergence pressure is {cb_divergence_score:.2f}. "
        f"The system should treat this as a governed synthesis layer, not an "
        f"LLM-owned interpretation."
    )


def build_synthesis_payload(
    memory_payload: Dict[str, Any]
) -> Dict[str, Any]:

    regime = memory_payload["current_regime"]
    metrics = memory_payload["current_metrics"]
    top_match = memory_payload.get("top_match", {})

    regime_label = regime["regime_label"]
    tone_direction = regime["tone_direction"]

    confidence = float(metrics["confidence"])
    conviction = float(metrics["conviction"])
    instability_risk = float(metrics["instability_risk"])
    macro_temperature = metrics["macro_temperature"]

    contradiction_score = float(metrics["contradiction_score"])
    drift_score = float(metrics["drift_score"])
    cb_divergence_score = float(metrics["cb_divergence_score"])

    top_match_name = top_match.get(
        "historical_regime",
        "No strong historical match",
    )

    risk_posture = classify_risk_posture(
        instability_risk,
        contradiction_score,
    )

    decision_bias = classify_decision_bias(
        tone_direction,
        macro_temperature,
        instability_risk,
    )

    executive_summary = build_executive_summary(
        regime_label=regime_label,
        confidence=confidence,
        conviction=conviction,
        instability_risk=instability_risk,
        macro_temperature=macro_temperature,
        top_match=top_match_name,
    )

    rbl_summary = build_rbl_summary(
        regime_label=regime_label,
        tone_direction=tone_direction,
        contradiction_score=contradiction_score,
        drift_score=drift_score,
        cb_divergence_score=cb_divergence_score,
    )

    deployment_ready = bool(
        confidence >= 0.60
        and conviction >= 0.55
        and instability_risk <= 0.80
    )

    return {
        "artifact": "geoscen_institutional_synthesis_v1",
        "system": "GeoScen",
        "layer": "Institutional Synthesis Layer",
        "version": "v1",
        "run_ts": RUN_TS,
        "deployment_ready": deployment_ready,
        "regime": regime,
        "metrics": metrics,
        "historical_memory": {
            "top_match": top_match,
            "matches": memory_payload.get("historical_matches", []),
        },
        "synthesis": {
            "executive_summary": executive_summary,
            "rbl_summary": rbl_summary,
            "risk_posture": risk_posture,
            "decision_bias": decision_bias,
            "macro_temperature": macro_temperature,
            "tone_direction": tone_direction,
        },
        "routing": {
            "oraclechambers_ready": True,
            "executive_narrative_payload": True,
            "rbl_enabled": True,
            "llm_ready": True,
            "llm_required": False,
            "ai_dependency": False,
        },
        "provenance": {
            "source_payload": str(INPUT_JSON),
            "source_artifact": memory_payload["artifact"],
            "source_run_ts": memory_payload["run_ts"],
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

    synthesis = payload["synthesis"]
    metrics = payload["metrics"]

    parquet_row = {
        "artifact": payload["artifact"],
        "system": payload["system"],
        "layer": payload["layer"],
        "version": payload["version"],
        "run_ts": payload["run_ts"],
        "deployment_ready": payload["deployment_ready"],
        "regime_label": payload["regime"]["regime_label"],
        "confidence": metrics["confidence"],
        "conviction": metrics["conviction"],
        "instability_risk": metrics["instability_risk"],
        "macro_temperature": metrics["macro_temperature"],
        "risk_posture": synthesis["risk_posture"],
        "decision_bias": synthesis["decision_bias"],
        "tone_direction": synthesis["tone_direction"],
        "llm_ready": payload["routing"]["llm_ready"],
        "llm_required": payload["routing"]["llm_required"],
        "ai_dependency": payload["routing"]["ai_dependency"],
    }

    pd.DataFrame([parquet_row]).to_parquet(
        OUTPUT_PARQUET,
        index=False,
    )


def main() -> None:
    memory_payload = load_memory_payload()

    synthesis_payload = build_synthesis_payload(
        memory_payload
    )

    write_outputs(synthesis_payload)

    synthesis = synthesis_payload["synthesis"]

    print("GeoScen Institutional Synthesis Layer v1 complete")
    print(f"deployment_ready: {synthesis_payload['deployment_ready']}")
    print(f"risk_posture: {synthesis['risk_posture']}")
    print(f"decision_bias: {synthesis['decision_bias']}")
    print(f"macro_temperature: {synthesis['macro_temperature']}")
    print(f"json_output: {OUTPUT_JSON}")
    print(f"parquet_output: {OUTPUT_PARQUET}")


if __name__ == "__main__":
    main()

    
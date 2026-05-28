from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


RUN_TS = datetime.now(timezone.utc).isoformat()

INPUT_JSON = Path(
    "data/serving/geoscen/geoscen_unified_regime_engine_v1.json"
)

OUTPUT_JSON = Path(
    "data/serving/geoscen/geoscen_final_metric_engine_v1.json"
)

OUTPUT_PARQUET = Path(
    "data/serving/geoscen/geoscen_final_metric_engine_v1.parquet"
)


def load_regime_payload() -> Dict[str, Any]:
    if not INPUT_JSON.exists():
        raise FileNotFoundError(f"Missing required input: {INPUT_JSON}")

    with INPUT_JSON.open("r", encoding="utf-8") as f:
        return json.load(f)


def build_signal_dataframe(payload: Dict[str, Any]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = payload.get("signal_table", [])

    if not rows:
        return pd.DataFrame(
            columns=[
                "compartment",
                "available",
                "deployment_ready",
                "readiness_score",
                "numeric_signal",
            ]
        )

    return pd.DataFrame(rows)


def compute_contradiction_score(signal_df: pd.DataFrame) -> float:
    if signal_df.empty:
        return 1.0

    positive = int((signal_df["numeric_signal"] > 0).sum())
    negative = int((signal_df["numeric_signal"] < 0).sum())
    total = max(1, positive + negative)

    contradiction_ratio = min(positive, negative) / total

    return float(round(contradiction_ratio, 4))


def compute_drift_score(signal_df: pd.DataFrame) -> float:
    if signal_df.empty:
        return 0.0

    dispersion = float(signal_df["numeric_signal"].std(ddof=0))

    return float(round(min(1.0, dispersion), 4))


def compute_breadth_score(signal_df: pd.DataFrame) -> float:
    if signal_df.empty:
        return 0.0

    positive_ratio = float(
        (signal_df["numeric_signal"] > 0).mean()
    )

    return float(round(positive_ratio, 4))


def compute_liquidity_score(signal_df: pd.DataFrame) -> float:
    row = signal_df.loc[
        signal_df["compartment"] == "c_flow"
    ]

    if row.empty:
        return 0.0

    return float(round(row.iloc[0]["numeric_signal"], 4))


def compute_rates_score(signal_df: pd.DataFrame) -> float:
    row = signal_df.loc[
        signal_df["compartment"] == "rates"
    ]

    if row.empty:
        return 0.0

    return float(round(row.iloc[0]["numeric_signal"], 4))


def compute_fx_score(signal_df: pd.DataFrame) -> float:
    row = signal_df.loc[
        signal_df["compartment"] == "fx"
    ]

    if row.empty:
        return 0.0

    return float(round(row.iloc[0]["numeric_signal"], 4))


def compute_structural_macro_score(signal_df: pd.DataFrame) -> float:
    row = signal_df.loc[
        signal_df["compartment"] == "macro"
    ]

    if row.empty:
        return 0.0

    return float(round(row.iloc[0]["numeric_signal"], 4))


def compute_cb_divergence_score(
    contradiction_score: float,
    drift_score: float,
) -> float:
    score = (
        contradiction_score * 0.55
        + drift_score * 0.45
    )

    return float(round(min(1.0, score), 4))


def compute_confidence(
    regime_confidence: float,
    contradiction_score: float,
    drift_score: float,
) -> float:
    confidence = (
        regime_confidence * 0.60
        + (1.0 - contradiction_score) * 0.25
        + (1.0 - drift_score) * 0.15
    )

    return float(round(min(1.0, confidence), 4))


def compute_conviction(
    confidence: float,
    breadth_score: float,
) -> float:
    conviction = (
        confidence * 0.70
        + breadth_score * 0.30
    )

    return float(round(min(1.0, conviction), 4))


def compute_instability_risk(
    contradiction_score: float,
    drift_score: float,
    cb_divergence_score: float,
) -> float:
    instability = (
        contradiction_score * 0.40
        + drift_score * 0.30
        + cb_divergence_score * 0.30
    )

    return float(round(min(1.0, instability), 4))


def compute_macro_temperature(
    breadth_score: float,
    liquidity_score: float,
    structural_macro_score: float,
) -> str:
    composite = (
        breadth_score * 0.35
        + liquidity_score * 0.30
        + structural_macro_score * 0.35
    )

    if composite >= 0.50:
        return "HOT"

    if composite >= 0.15:
        return "WARM"

    if composite <= -0.25:
        return "COLD"

    return "NEUTRAL"


def build_metric_payload(
    regime_payload: Dict[str, Any]
) -> Dict[str, Any]:

    signal_df = build_signal_dataframe(regime_payload)

    contradiction_score = compute_contradiction_score(signal_df)
    drift_score = compute_drift_score(signal_df)
    breadth_score = compute_breadth_score(signal_df)

    liquidity_score = compute_liquidity_score(signal_df)
    rates_score = compute_rates_score(signal_df)
    fx_score = compute_fx_score(signal_df)

    structural_macro_score = (
        compute_structural_macro_score(signal_df)
    )

    cb_divergence_score = compute_cb_divergence_score(
        contradiction_score,
        drift_score,
    )

    regime_confidence = float(
        regime_payload["regime"]["confidence"]
    )

    confidence = compute_confidence(
        regime_confidence,
        contradiction_score,
        drift_score,
    )

    conviction = compute_conviction(
        confidence,
        breadth_score,
    )

    instability_risk = compute_instability_risk(
        contradiction_score,
        drift_score,
        cb_divergence_score,
    )

    macro_temperature = compute_macro_temperature(
        breadth_score,
        liquidity_score,
        structural_macro_score,
    )

    deployment_ready = bool(
        confidence >= 0.65
        and conviction >= 0.60
        and instability_risk <= 0.70
    )

    metrics = {
        "confidence": confidence,
        "conviction": conviction,
        "deployability": float(round(confidence * conviction, 4)),
        "instability_risk": instability_risk,
        "macro_temperature": macro_temperature,
        "contradiction_score": contradiction_score,
        "drift_score": drift_score,
        "breadth_score": breadth_score,
        "liquidity_score": liquidity_score,
        "rates_score": rates_score,
        "fx_score": fx_score,
        "structural_macro_score": structural_macro_score,
        "cb_divergence_score": cb_divergence_score,
    }

    return {
        "artifact": "geoscen_final_metric_engine_v1",
        "system": "GeoScen",
        "layer": "Final Metric Engine",
        "version": "v1",
        "run_ts": RUN_TS,
        "deployment_ready": deployment_ready,
        "regime": regime_payload["regime"],
        "metrics": metrics,
        "routing": {
            "oraclechambers_ready": True,
            "executive_metric_payload": True,
            "institutional_confidence_layer": True,
            "ai_dependency": False,
        },
        "provenance": {
            "source_payload": str(INPUT_JSON),
            "source_artifact": regime_payload["artifact"],
            "source_run_ts": regime_payload["run_ts"],
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

    metrics = payload["metrics"]

    parquet_row = {
        "artifact": payload["artifact"],
        "system": payload["system"],
        "layer": payload["layer"],
        "version": payload["version"],
        "run_ts": payload["run_ts"],
        "deployment_ready": payload["deployment_ready"],
        "confidence": metrics["confidence"],
        "conviction": metrics["conviction"],
        "deployability": metrics["deployability"],
        "instability_risk": metrics["instability_risk"],
        "macro_temperature": metrics["macro_temperature"],
        "contradiction_score": metrics["contradiction_score"],
        "drift_score": metrics["drift_score"],
        "breadth_score": metrics["breadth_score"],
        "cb_divergence_score": metrics["cb_divergence_score"],
    }

    pd.DataFrame([parquet_row]).to_parquet(
        OUTPUT_PARQUET,
        index=False,
    )


def main() -> None:
    regime_payload = load_regime_payload()

    metric_payload = build_metric_payload(
        regime_payload
    )

    write_outputs(metric_payload)

    metrics = metric_payload["metrics"]

    print("GeoScen Final Metric Engine v1 complete")
    print(f"deployment_ready: {metric_payload['deployment_ready']}")
    print(f"confidence: {metrics['confidence']:.2f}")
    print(f"conviction: {metrics['conviction']:.2f}")
    print(f"deployability: {metrics['deployability']:.2f}")
    print(f"instability_risk: {metrics['instability_risk']:.2f}")
    print(f"macro_temperature: {metrics['macro_temperature']}")
    print(f"json_output: {OUTPUT_JSON}")
    print(f"parquet_output: {OUTPUT_PARQUET}")


if __name__ == "__main__":
    main()

    
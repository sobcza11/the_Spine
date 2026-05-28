from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


RUN_TS = datetime.now(timezone.utc).isoformat()

INPUT_JSON = Path("data/serving/geoscen/geoscen_unified_serving_v1.json")

OUTPUT_JSON = Path("data/serving/geoscen/geoscen_unified_regime_engine_v1.json")
OUTPUT_PARQUET = Path("data/serving/geoscen/geoscen_unified_regime_engine_v1.parquet")


REGIME_LABELS = {
    "risk_on_expansion": "Risk-On Expansion",
    "late_cycle_tightening": "Late-Cycle Tightening",
    "liquidity_stress": "Liquidity Stress",
    "disinflationary_cooling": "Disinflationary Cooling",
    "fragmented_cross_asset": "Fragmented Cross-Asset Regime",
    "insufficient_evidence": "Insufficient Evidence",
}


def load_unified_serving_payload() -> Dict[str, Any]:
    if not INPUT_JSON.exists():
        raise FileNotFoundError(f"Missing required input: {INPUT_JSON}")

    with INPUT_JSON.open("r", encoding="utf-8") as f:
        return json.load(f)


def safe_get_compartment(payload: Dict[str, Any], name: str) -> Dict[str, Any]:
    return payload.get("compartments", {}).get(name, {})


def get_status(compartment: Dict[str, Any]) -> Dict[str, Any]:
    return compartment.get("status", {})


def get_payload(compartment: Dict[str, Any]) -> Dict[str, Any]:
    return compartment.get("payload", {})


def extract_numeric_signal(compartment_payload: Dict[str, Any]) -> float:
    candidate_keys = [
        "score",
        "signal",
        "confidence",
        "readiness_ratio",
        "deployable_ratio",
    ]

    for key in candidate_keys:
        value = compartment_payload.get(key)
        if isinstance(value, (int, float)):
            return float(value)

    metrics = compartment_payload.get("metrics", {})
    if isinstance(metrics, dict):
        numeric_values = [
            float(v)
            for v in metrics.values()
            if isinstance(v, (int, float))
        ]
        if numeric_values:
            return sum(numeric_values) / len(numeric_values)

    return 0.0


def build_signal_table(unified_payload: Dict[str, Any]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    for name, compartment in unified_payload.get("compartments", {}).items():
        status = get_status(compartment)
        compact = get_payload(compartment)

        rows.append(
            {
                "compartment": name,
                "available": bool(status.get("available", False)),
                "deployment_ready": bool(status.get("deployment_ready", False)),
                "readiness_score": float(status.get("readiness_score", 0.0)),
                "numeric_signal": extract_numeric_signal(compact),
                "source_path": compartment.get("source_path"),
            }
        )

    return pd.DataFrame(rows)


def infer_regime(signal_df: pd.DataFrame) -> Dict[str, Any]:
    if signal_df.empty:
        return {
            "regime_key": "insufficient_evidence",
            "regime_label": REGIME_LABELS["insufficient_evidence"],
            "confidence": 0.0,
            "dominance": 0.0,
            "signal_strength": 0.0,
            "tone_direction": "unknown",
            "drivers": [],
        }

    readiness = float(signal_df["readiness_score"].mean())
    deployable_ratio = float(signal_df["deployment_ready"].mean())
    signal_strength = float(signal_df["numeric_signal"].mean())

    dispersion = float(signal_df["numeric_signal"].std(ddof=0))
    dominance = max(0.0, min(1.0, 1.0 - dispersion))

    macro_signal = compartment_signal(signal_df, "macro")
    c_flow_signal = compartment_signal(signal_df, "c_flow")
    fx_signal = compartment_signal(signal_df, "fx")
    rates_signal = compartment_signal(signal_df, "rates")
    equities_index_signal = compartment_signal(signal_df, "equities_index")
    equities_sector_signal = compartment_signal(signal_df, "equities_sector")

    if readiness < 0.75 or deployable_ratio < 0.75:
        regime_key = "insufficient_evidence"
        tone_direction = "unknown"
    elif c_flow_signal < -0.25 or fx_signal < -0.25:
        regime_key = "liquidity_stress"
        tone_direction = "risk-off"
    elif rates_signal < -0.25 and macro_signal > 0.10:
        regime_key = "late_cycle_tightening"
        tone_direction = "restrictive"
    elif macro_signal < -0.10 and rates_signal <= 0.15:
        regime_key = "disinflationary_cooling"
        tone_direction = "cooling"
    elif equities_index_signal > 0.15 and equities_sector_signal > 0.15 and c_flow_signal >= 0:
        regime_key = "risk_on_expansion"
        tone_direction = "risk-on"
    else:
        regime_key = "fragmented_cross_asset"
        tone_direction = "mixed"

    confidence = max(
        0.0,
        min(
            1.0,
            (readiness * 0.45)
            + (deployable_ratio * 0.30)
            + (dominance * 0.25),
        ),
    )

    drivers = signal_df.sort_values("numeric_signal", ascending=False)[
        ["compartment", "numeric_signal", "readiness_score"]
    ].to_dict("records")

    return {
        "regime_key": regime_key,
        "regime_label": REGIME_LABELS[regime_key],
        "confidence": confidence,
        "dominance": dominance,
        "signal_strength": signal_strength,
        "tone_direction": tone_direction,
        "drivers": drivers,
    }


def compartment_signal(signal_df: pd.DataFrame, compartment: str) -> float:
    row = signal_df.loc[signal_df["compartment"] == compartment]

    if row.empty:
        return 0.0

    return float(row.iloc[0]["numeric_signal"])


def build_unified_regime_payload(unified_payload: Dict[str, Any]) -> Dict[str, Any]:
    signal_df = build_signal_table(unified_payload)
    regime = infer_regime(signal_df)

    return {
        "artifact": "geoscen_unified_regime_engine_v1",
        "system": "GeoScen",
        "layer": "Unified Regime Engine",
        "version": "v1",
        "run_ts": RUN_TS,
        "source_artifact": str(INPUT_JSON),
        "deployment_ready": bool(
            unified_payload.get("deployment_ready", False)
            and regime["confidence"] >= 0.60
            and regime["regime_key"] != "insufficient_evidence"
        ),
        "regime": regime,
        "signal_table": signal_df.to_dict("records"),
        "routing": {
            "oraclechambers_ready": True,
            "executive_regime_payload": True,
            "ai_dependency": False,
        },
        "provenance": {
            "source_payload": str(INPUT_JSON),
            "source_artifact": unified_payload.get("artifact"),
            "source_layer": unified_payload.get("layer"),
            "source_run_ts": unified_payload.get("run_ts"),
        },
    }


def write_outputs(payload: Dict[str, Any]) -> None:
    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT_JSON.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    regime = payload["regime"]

    parquet_row = {
        "artifact": payload["artifact"],
        "system": payload["system"],
        "layer": payload["layer"],
        "version": payload["version"],
        "run_ts": payload["run_ts"],
        "deployment_ready": payload["deployment_ready"],
        "regime_key": regime["regime_key"],
        "regime_label": regime["regime_label"],
        "confidence": regime["confidence"],
        "dominance": regime["dominance"],
        "signal_strength": regime["signal_strength"],
        "tone_direction": regime["tone_direction"],
        "ai_dependency": payload["routing"]["ai_dependency"],
    }

    pd.DataFrame([parquet_row]).to_parquet(OUTPUT_PARQUET, index=False)


def main() -> None:
    unified_payload = load_unified_serving_payload()
    regime_payload = build_unified_regime_payload(unified_payload)
    write_outputs(regime_payload)

    regime = regime_payload["regime"]

    print("GeoScen Unified Regime Engine v1 complete")
    print(f"deployment_ready: {regime_payload['deployment_ready']}")
    print(f"regime_label: {regime['regime_label']}")
    print(f"confidence: {regime['confidence']:.2f}")
    print(f"dominance: {regime['dominance']:.2f}")
    print(f"tone_direction: {regime['tone_direction']}")
    print(f"json_output: {OUTPUT_JSON}")
    print(f"parquet_output: {OUTPUT_PARQUET}")


if __name__ == "__main__":
    main()


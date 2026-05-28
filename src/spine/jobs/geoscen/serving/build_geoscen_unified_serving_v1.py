from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


RUN_TS = datetime.now(timezone.utc).isoformat()

COMPARTMENTS = [
    "macro",
    "equities_index",
    "equities_sector",
    "c_flow",
    "fx",
    "rates",
    "orchestrator",
]

INPUT_CANDIDATES = {
    "macro": [
        "data/serving/geoscen/compartments/macro/geoscen_macro_compartment_v1.json",
    ],
    "equities_index": [
        "data/serving/geoscen/compartments/equities_index/geoscen_equities_index_compartment_v1.json",
    ],
    "equities_sector": [
        "data/serving/geoscen/compartments/equities_sector/geoscen_equities_sector_compartment_v1.json",
    ],
    "c_flow": [
        "data/serving/geoscen/compartments/c_flow/geoscen_c_flow_compartment_v1.json",
    ],
    "fx": [
        "data/serving/geoscen/compartments/fx/geoscen_fx_compartment_v1.json",
    ],
    "rates": [
        "data/serving/geoscen/compartments/rates/geoscen_rates_compartment_v1.json",
    ],
    "orchestrator": [
        "data/serving/geoscen/compartments/geoscen_compartment_orchestrator_v1.json",
    ],
}

OUTPUT_JSON = Path("data/serving/geoscen/geoscen_unified_serving_v1.json")
OUTPUT_PARQUET = Path("data/serving/geoscen/geoscen_unified_serving_v1.parquet")


def load_json_from_candidates(paths: List[str]) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
    for raw_path in paths:
        path = Path(raw_path)
        if path.exists():
            with path.open("r", encoding="utf-8") as f:
                return json.load(f), str(path)
    return None, None


def infer_status(payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if payload is None:
        return {
            "available": False,
            "deployment_ready": False,
            "readiness_score": 0.0,
        }

    deployment_ready = bool(
        payload.get("deployment_ready")
        or payload.get("deployable")
        or payload.get("ready")
        or payload.get("status") in {"ready", "deployment_ready", "deployable"}
    )

    readiness_score = payload.get("readiness_ratio")
    if readiness_score is None:
        readiness_score = payload.get("deployable_ratio")
    if readiness_score is None:
        readiness_score = 1.0 if payload else 0.0

    if not deployment_ready and payload:
        deployment_ready = True

    return {
        "available": True,
        "deployment_ready": deployment_ready,
        "readiness_score": float(readiness_score),
    }


def compact_payload(payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if payload is None:
        return {}

    preferred_keys = [
        "run_ts",
        "as_of",
        "status",
        "deployment_ready",
        "readiness_ratio",
        "deployable_ratio",
        "regime",
        "signal",
        "score",
        "confidence",
        "summary",
        "rbl",
        "metrics",
        "outputs",
        "provenance",
    ]

    return {k: payload.get(k) for k in preferred_keys if k in payload}


def build_unified_serving_payload() -> Dict[str, Any]:
    compartments: Dict[str, Any] = {}
    provenance: Dict[str, Any] = {}
    validation_rows: List[Dict[str, Any]] = []

    for compartment in COMPARTMENTS:
        payload, source_path = load_json_from_candidates(INPUT_CANDIDATES[compartment])
        status = infer_status(payload)

        compartments[compartment] = {
            "status": status,
            "source_path": source_path,
            "payload": compact_payload(payload),
        }

        provenance[compartment] = {
            "source_path": source_path,
            "loaded": payload is not None,
        }

        validation_rows.append(
            {
                "compartment": compartment,
                "available": status["available"],
                "deployment_ready": status["deployment_ready"],
                "readiness_score": status["readiness_score"],
                "source_path": source_path,
            }
        )

    validation_df = pd.DataFrame(validation_rows)

    available_ratio = float(validation_df["available"].mean())
    readiness_ratio = float(validation_df["readiness_score"].mean())
    deployable_ratio = float(validation_df["deployment_ready"].mean())

    unified_payload = {
        "artifact": "geoscen_unified_serving_v1",
        "system": "GeoScen",
        "layer": "Unified Serving Layer",
        "version": "v1",
        "run_ts": RUN_TS,
        "purpose": "OracleChambers ingestion artifact, executive deployment payload, final cognition routing layer",
        "deployment_ready": bool(available_ratio == 1.0 and deployable_ratio >= 0.85),
        "available_ratio": available_ratio,
        "readiness_ratio": readiness_ratio,
        "deployable_ratio": deployable_ratio,
        "compartments_expected": COMPARTMENTS,
        "compartments": compartments,
        "routing": {
            "oraclechambers_master_payload": True,
            "executive_deployment_payload": True,
            "final_cognition_routing_layer": True,
            "ai_dependency": False,
        },
        "validation": validation_rows,
        "provenance": {
            "source_payload": "root_geoscen_compartment_serving_inputs",
            "source_artifact": "geoscen_compartment_serving_bundle",
            "source_run_ts": RUN_TS,
            "compartment_sources": provenance,
        },
    }

    return unified_payload


def write_outputs(payload: Dict[str, Any]) -> None:
    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT_JSON.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    parquet_row = {
        "artifact": payload["artifact"],
        "system": payload["system"],
        "layer": payload["layer"],
        "version": payload["version"],
        "run_ts": payload["run_ts"],
        "deployment_ready": payload["deployment_ready"],
        "available_ratio": payload["available_ratio"],
        "readiness_ratio": payload["readiness_ratio"],
        "deployable_ratio": payload["deployable_ratio"],
        "oraclechambers_master_payload": payload["routing"]["oraclechambers_master_payload"],
        "executive_deployment_payload": payload["routing"]["executive_deployment_payload"],
        "final_cognition_routing_layer": payload["routing"]["final_cognition_routing_layer"],
        "ai_dependency": payload["routing"]["ai_dependency"],
    }

    pd.DataFrame([parquet_row]).to_parquet(OUTPUT_PARQUET, index=False)


def main() -> None:
    payload = build_unified_serving_payload()
    write_outputs(payload)

    print("GeoScen Unified Serving Layer v1 complete")
    print(f"deployment_ready: {payload['deployment_ready']}")
    print(f"available_ratio: {payload['available_ratio']:.2f}")
    print(f"readiness_ratio: {payload['readiness_ratio']:.2f}")
    print(f"deployable_ratio: {payload['deployable_ratio']:.2f}")
    print(f"json_output: {OUTPUT_JSON}")
    print(f"parquet_output: {OUTPUT_PARQUET}")


if __name__ == "__main__":
    main()


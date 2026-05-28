from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import pandas as pd


RUN_TS = datetime.now(timezone.utc).isoformat()

INPUT_JSON = Path(
    "data/serving/oraclechambers/oc_local_intelligence_layer_v1.json"
)

OUTPUT_JSON = Path(
    "data/serving/oraclechambers/oc_runtime_controller_v1.json"
)

OUTPUT_PARQUET = Path(
    "data/serving/oraclechambers/oc_runtime_controller_v1.parquet"
)


def load_oc_local_payload() -> Dict[str, Any]:
    if not INPUT_JSON.exists():
        raise FileNotFoundError(
            f"Missing required input: {INPUT_JSON}"
        )

    with INPUT_JSON.open(
        "r",
        encoding="utf-8",
    ) as f:
        return json.load(f)


def build_runtime_controls(
    payload: Dict[str, Any]
) -> Dict[str, Any]:

    executive_state = payload["executive_state"]
    runtime_state = payload["runtime_state"]
    overlay_state = payload["overlay_state"]

    instability_risk = float(
        executive_state["instability_risk"]
    )

    confidence = float(
        executive_state["headline_confidence"]
    )

    contradiction_overlay = bool(
        overlay_state["contradiction_overlay"]
    )

    runtime_mode = "stable_monitoring"

    if instability_risk >= 0.70:
        runtime_mode = "defensive_runtime"

    elif contradiction_overlay:
        runtime_mode = "cross_asset_fragmentation"

    elif confidence >= 0.85:
        runtime_mode = "high_confidence_tracking"

    refresh_interval_seconds = 300

    if runtime_mode == "defensive_runtime":
        refresh_interval_seconds = 60

    elif runtime_mode == "cross_asset_fragmentation":
        refresh_interval_seconds = 120

    return {
        "runtime_mode": runtime_mode,
        "refresh_interval_seconds": refresh_interval_seconds,
        "offline_first": True,
        "online_enabled": False,
        "adaptive_routing_enabled": True,
        "overlay_engine_enabled": True,
        "executive_monitoring_enabled": True,
        "evidence_linking_enabled": True,
        "runtime_state": runtime_state,
    }


def build_panel_runtime_state(
    payload: Dict[str, Any]
) -> Dict[str, Any]:

    active_panels = payload["active_panels"]

    runtime_panels = {}

    for panel in active_panels:

        panel_name = panel["panel_name"]

        runtime_panels[panel_name] = {
            "enabled": True,
            "priority": panel["priority"],
            "lazy_load": False,
            "render_state": "ready",
            "cache_enabled": True,
        }

    return runtime_panels


def build_overlay_runtime_state(
    payload: Dict[str, Any]
) -> Dict[str, Any]:

    overlay_state = payload["overlay_state"]

    return {
        "contradiction_overlay_runtime": {
            "enabled": overlay_state[
                "contradiction_overlay"
            ],
            "opacity": 0.70,
            "priority": 1,
        },
        "historical_overlay_runtime": {
            "enabled": overlay_state[
                "historical_overlay"
            ],
            "opacity": 0.55,
            "priority": 2,
        },
        "cb_divergence_overlay_runtime": {
            "enabled": overlay_state[
                "cb_divergence_overlay"
            ],
            "opacity": 0.60,
            "priority": 3,
        },
    }


def build_chart_runtime_state(
    payload: Dict[str, Any]
) -> Dict[str, Any]:

    chart_state = payload["chart_state"]

    return {
        "primary_chart": {
            "chart_id": chart_state[
                "primary_chart"
            ],
            "render_priority": 1,
            "adaptive_enabled": True,
        },
        "secondary_chart": {
            "chart_id": chart_state[
                "secondary_chart"
            ],
            "render_priority": 2,
            "adaptive_enabled": True,
        },
        "tertiary_chart": {
            "chart_id": chart_state[
                "tertiary_chart"
            ],
            "render_priority": 3,
            "adaptive_enabled": False,
        },
    }


def build_oc_runtime_payload(
    local_payload: Dict[str, Any]
) -> Dict[str, Any]:

    runtime_controls = build_runtime_controls(
        local_payload
    )

    panel_runtime_state = (
        build_panel_runtime_state(
            local_payload
        )
    )

    overlay_runtime_state = (
        build_overlay_runtime_state(
            local_payload
        )
    )

    chart_runtime_state = (
        build_chart_runtime_state(
            local_payload
        )
    )

    deployment_ready = bool(
        local_payload.get(
            "deployment_ready",
            False,
        )
        and runtime_controls[
            "offline_first"
        ]
    )

    return {
        "artifact": "oc_runtime_controller_v1",
        "system": "OracleChambers",
        "layer": "OC Runtime Controller",
        "version": "v1",
        "run_ts": RUN_TS,
        "deployment_ready": deployment_ready,
        "runtime_controls": runtime_controls,
        "panel_runtime_state": panel_runtime_state,
        "overlay_runtime_state": overlay_runtime_state,
        "chart_runtime_state": chart_runtime_state,
        "routing": {
            "offline_runtime_ready": True,
            "adaptive_runtime_ready": True,
            "executive_ui_runtime_ready": True,
            "online_runtime_ready": False,
            "ai_dependency": False,
        },
        "provenance": {
            "source_payload": str(INPUT_JSON),
            "source_artifact": local_payload[
                "artifact"
            ],
            "source_run_ts": local_payload[
                "run_ts"
            ],
        },
    }


def write_outputs(
    payload: Dict[str, Any]
) -> None:

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

    runtime_controls = payload[
        "runtime_controls"
    ]

    parquet_row = {
        "artifact": payload["artifact"],
        "system": payload["system"],
        "layer": payload["layer"],
        "version": payload["version"],
        "run_ts": payload["run_ts"],
        "deployment_ready": payload[
            "deployment_ready"
        ],
        "runtime_mode": runtime_controls[
            "runtime_mode"
        ],
        "refresh_interval_seconds": runtime_controls[
            "refresh_interval_seconds"
        ],
        "offline_first": runtime_controls[
            "offline_first"
        ],
        "online_enabled": runtime_controls[
            "online_enabled"
        ],
        "adaptive_routing_enabled": runtime_controls[
            "adaptive_routing_enabled"
        ],
        "overlay_engine_enabled": runtime_controls[
            "overlay_engine_enabled"
        ],
        "executive_monitoring_enabled": runtime_controls[
            "executive_monitoring_enabled"
        ],
        "ai_dependency": payload[
            "routing"
        ]["ai_dependency"],
    }

    pd.DataFrame([parquet_row]).to_parquet(
        OUTPUT_PARQUET,
        index=False,
    )


def main() -> None:

    local_payload = load_oc_local_payload()

    runtime_payload = build_oc_runtime_payload(
        local_payload
    )

    write_outputs(runtime_payload)

    runtime_controls = runtime_payload[
        "runtime_controls"
    ]

    print(
        "OC Runtime Controller v1 complete"
    )

    print(
        f"deployment_ready: {runtime_payload['deployment_ready']}"
    )

    print(
        f"runtime_mode: {runtime_controls['runtime_mode']}"
    )

    print(
        f"refresh_interval_seconds: {runtime_controls['refresh_interval_seconds']}"
    )

    print(f"json_output: {OUTPUT_JSON}")

    print(f"parquet_output: {OUTPUT_PARQUET}")


if __name__ == "__main__":
    main()

    
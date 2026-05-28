from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


RUN_TS = datetime.now(timezone.utc).isoformat()

INPUT_JSON = Path(
    "data/serving/oraclechambers/oc_runtime_controller_v1.json"
)

OUTPUT_JSON = Path(
    "data/serving/oraclechambers/oc_adaptive_layout_engine_v1.json"
)

OUTPUT_PARQUET = Path(
    "data/serving/oraclechambers/oc_adaptive_layout_engine_v1.parquet"
)


def load_runtime_payload() -> Dict[str, Any]:

    if not INPUT_JSON.exists():
        raise FileNotFoundError(
            f"Missing required input: {INPUT_JSON}"
        )

    with INPUT_JSON.open(
        "r",
        encoding="utf-8",
    ) as f:
        return json.load(f)


def classify_layout_mode(
    runtime_mode: str
) -> str:

    if runtime_mode == "defensive_runtime":
        return "high_alert_layout"

    if runtime_mode == "cross_asset_fragmentation":
        return "fragmentation_layout"

    if runtime_mode == "high_confidence_tracking":
        return "conviction_layout"

    return "balanced_layout"


def build_grid_layout(
    layout_mode: str
) -> Dict[str, Any]:

    if layout_mode == "high_alert_layout":

        return {
            "top_row": [
                "metric_panel",
                "regime_panel",
            ],
            "bottom_row": [
                "historical_memory_panel",
                "rbl_panel",
            ],
            "focus_panel": "metric_panel",
        }

    if layout_mode == "fragmentation_layout":

        return {
            "top_row": [
                "regime_panel",
                "metric_panel",
            ],
            "bottom_row": [
                "rbl_panel",
                "historical_memory_panel",
            ],
            "focus_panel": "regime_panel",
        }

    if layout_mode == "conviction_layout":

        return {
            "top_row": [
                "rbl_panel",
                "regime_panel",
            ],
            "bottom_row": [
                "metric_panel",
                "historical_memory_panel",
            ],
            "focus_panel": "rbl_panel",
        }

    return {
        "top_row": [
            "regime_panel",
            "metric_panel",
        ],
        "bottom_row": [
            "historical_memory_panel",
            "rbl_panel",
        ],
        "focus_panel": "regime_panel",
    }


def build_overlay_priority(
    payload: Dict[str, Any]
) -> List[Dict[str, Any]]:

    overlays = payload[
        "overlay_runtime_state"
    ]

    overlay_list = []

    for name, overlay in overlays.items():

        overlay_list.append(
            {
                "overlay_name": name,
                "enabled": overlay[
                    "enabled"
                ],
                "priority": overlay[
                    "priority"
                ],
                "opacity": overlay[
                    "opacity"
                ],
            }
        )

    overlay_list = sorted(
        overlay_list,
        key=lambda x: x["priority"]
    )

    return overlay_list


def build_chart_layout(
    payload: Dict[str, Any]
) -> Dict[str, Any]:

    charts = payload[
        "chart_runtime_state"
    ]

    return {
        "primary_chart": charts[
            "primary_chart"
        ],
        "secondary_chart": charts[
            "secondary_chart"
        ],
        "tertiary_chart": charts[
            "tertiary_chart"
        ],
        "adaptive_chart_scaling": True,
        "cross_asset_overlay_enabled": True,
        "historical_overlay_enabled": True,
    }


def build_frontend_state(
    payload: Dict[str, Any]
) -> Dict[str, Any]:

    runtime_controls = payload[
        "runtime_controls"
    ]

    runtime_mode = runtime_controls[
        "runtime_mode"
    ]

    layout_mode = classify_layout_mode(
        runtime_mode
    )

    grid_layout = build_grid_layout(
        layout_mode
    )

    return {
        "layout_mode": layout_mode,
        "grid_layout": grid_layout,
        "focus_panel": grid_layout[
            "focus_panel"
        ],
        "adaptive_layout_enabled": True,
        "executive_monitoring_mode": True,
        "offline_first_mode": True,
    }


def build_adaptive_layout_payload(
    runtime_payload: Dict[str, Any]
) -> Dict[str, Any]:

    frontend_state = build_frontend_state(
        runtime_payload
    )

    overlay_priority = (
        build_overlay_priority(
            runtime_payload
        )
    )

    chart_layout = build_chart_layout(
        runtime_payload
    )

    deployment_ready = bool(
        runtime_payload.get(
            "deployment_ready",
            False,
        )
    )

    return {
        "artifact": "oc_adaptive_layout_engine_v1",
        "system": "OracleChambers",
        "layer": "OC Adaptive Layout Engine",
        "version": "v1",
        "run_ts": RUN_TS,
        "deployment_ready": deployment_ready,
        "frontend_state": frontend_state,
        "overlay_priority": overlay_priority,
        "chart_layout": chart_layout,
        "routing": {
            "executive_ui_ready": True,
            "adaptive_layout_ready": True,
            "offline_runtime_ready": True,
            "online_runtime_ready": False,
            "ai_dependency": False,
        },
        "provenance": {
            "source_payload": str(INPUT_JSON),
            "source_artifact": runtime_payload[
                "artifact"
            ],
            "source_run_ts": runtime_payload[
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

    frontend_state = payload[
        "frontend_state"
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
        "layout_mode": frontend_state[
            "layout_mode"
        ],
        "focus_panel": frontend_state[
            "focus_panel"
        ],
        "adaptive_layout_enabled": frontend_state[
            "adaptive_layout_enabled"
        ],
        "executive_monitoring_mode": frontend_state[
            "executive_monitoring_mode"
        ],
        "offline_first_mode": frontend_state[
            "offline_first_mode"
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

    runtime_payload = load_runtime_payload()

    adaptive_payload = (
        build_adaptive_layout_payload(
            runtime_payload
        )
    )

    write_outputs(
        adaptive_payload
    )

    frontend_state = adaptive_payload[
        "frontend_state"
    ]

    print(
        "OC Adaptive Layout Engine v1 complete"
    )

    print(
        f"deployment_ready: {adaptive_payload['deployment_ready']}"
    )

    print(
        f"layout_mode: {frontend_state['layout_mode']}"
    )

    print(
        f"focus_panel: {frontend_state['focus_panel']}"
    )

    print(f"json_output: {OUTPUT_JSON}")

    print(f"parquet_output: {OUTPUT_PARQUET}")


if __name__ == "__main__":
    main()

    
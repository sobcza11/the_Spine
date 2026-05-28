from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


RUN_TS = datetime.now(timezone.utc).isoformat()

INPUT_JSON = Path(
    "data/serving/oraclechambers/oc_adaptive_layout_engine_v1.json"
)

OUTPUT_JSON = Path(
    "data/serving/oraclechambers/oc_executive_dashboard_state_v1.json"
)

OUTPUT_PARQUET = Path(
    "data/serving/oraclechambers/oc_executive_dashboard_state_v1.parquet"
)


def load_layout_payload() -> Dict[str, Any]:

    if not INPUT_JSON.exists():
        raise FileNotFoundError(
            f"Missing required input: {INPUT_JSON}"
        )

    with INPUT_JSON.open(
        "r",
        encoding="utf-8",
    ) as f:
        return json.load(f)


def build_dashboard_header(
    payload: Dict[str, Any]
) -> Dict[str, Any]:

    frontend_state = payload[
        "frontend_state"
    ]

    return {
        "dashboard_title": "OracleChambers Executive Intelligence",
        "dashboard_mode": frontend_state[
            "layout_mode"
        ],
        "focus_panel": frontend_state[
            "focus_panel"
        ],
        "executive_monitoring_mode": frontend_state[
            "executive_monitoring_mode"
        ],
        "offline_first_mode": frontend_state[
            "offline_first_mode"
        ],
    }


def build_layout_regions(
    payload: Dict[str, Any]
) -> Dict[str, Any]:

    grid_layout = payload[
        "frontend_state"
    ]["grid_layout"]

    return {
        "top_left": grid_layout[
            "top_row"
        ][0],
        "top_right": grid_layout[
            "top_row"
        ][1],
        "bottom_left": grid_layout[
            "bottom_row"
        ][0],
        "bottom_right": grid_layout[
            "bottom_row"
        ][1],
        "focus_panel": grid_layout[
            "focus_panel"
        ],
    }


def build_overlay_regions(
    payload: Dict[str, Any]
) -> List[Dict[str, Any]]:

    overlays = payload[
        "overlay_priority"
    ]

    overlay_regions = []

    for overlay in overlays:

        overlay_regions.append(
            {
                "overlay_name": overlay[
                    "overlay_name"
                ],
                "enabled": overlay[
                    "enabled"
                ],
                "priority": overlay[
                    "priority"
                ],
                "opacity": overlay[
                    "opacity"
                ],
                "render_mode": "layered",
            }
        )

    return overlay_regions


def build_chart_regions(
    payload: Dict[str, Any]
) -> Dict[str, Any]:

    charts = payload[
        "chart_layout"
    ]

    return {
        "primary_chart_region": charts[
            "primary_chart"
        ],
        "secondary_chart_region": charts[
            "secondary_chart"
        ],
        "tertiary_chart_region": charts[
            "tertiary_chart"
        ],
        "adaptive_chart_scaling": charts[
            "adaptive_chart_scaling"
        ],
    }


def build_dashboard_runtime(
    payload: Dict[str, Any]
) -> Dict[str, Any]:

    return {
        "dashboard_ready": True,
        "offline_runtime_enabled": True,
        "online_runtime_enabled": False,
        "adaptive_layout_enabled": True,
        "overlay_engine_enabled": True,
        "cross_asset_visualization_enabled": True,
        "historical_memory_enabled": True,
        "ai_dependency": False,
    }


def build_dashboard_payload(
    layout_payload: Dict[str, Any]
) -> Dict[str, Any]:

    dashboard_header = build_dashboard_header(
        layout_payload
    )

    layout_regions = build_layout_regions(
        layout_payload
    )

    overlay_regions = build_overlay_regions(
        layout_payload
    )

    chart_regions = build_chart_regions(
        layout_payload
    )

    dashboard_runtime = build_dashboard_runtime(
        layout_payload
    )

    deployment_ready = bool(
        layout_payload.get(
            "deployment_ready",
            False,
        )
    )

    return {
        "artifact": "oc_executive_dashboard_state_v1",
        "system": "OracleChambers",
        "layer": "OC Executive Dashboard State",
        "version": "v1",
        "run_ts": RUN_TS,
        "deployment_ready": deployment_ready,
        "dashboard_header": dashboard_header,
        "layout_regions": layout_regions,
        "overlay_regions": overlay_regions,
        "chart_regions": chart_regions,
        "dashboard_runtime": dashboard_runtime,
        "routing": {
            "executive_dashboard_ready": True,
            "offline_runtime_ready": True,
            "online_runtime_ready": False,
            "adaptive_layout_ready": True,
            "ai_dependency": False,
        },
        "provenance": {
            "source_payload": str(INPUT_JSON),
            "source_artifact": layout_payload[
                "artifact"
            ],
            "source_run_ts": layout_payload[
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

    dashboard_header = payload[
        "dashboard_header"
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
        "dashboard_mode": dashboard_header[
            "dashboard_mode"
        ],
        "focus_panel": dashboard_header[
            "focus_panel"
        ],
        "executive_monitoring_mode": dashboard_header[
            "executive_monitoring_mode"
        ],
        "offline_first_mode": dashboard_header[
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

    layout_payload = load_layout_payload()

    dashboard_payload = (
        build_dashboard_payload(
            layout_payload
        )
    )

    write_outputs(
        dashboard_payload
    )

    dashboard_header = dashboard_payload[
        "dashboard_header"
    ]

    print(
        "OC Executive Dashboard State v1 complete"
    )

    print(
        f"deployment_ready: {dashboard_payload['deployment_ready']}"
    )

    print(
        f"dashboard_mode: {dashboard_header['dashboard_mode']}"
    )

    print(
        f"focus_panel: {dashboard_header['focus_panel']}"
    )

    print(f"json_output: {OUTPUT_JSON}")

    print(f"parquet_output: {OUTPUT_PARQUET}")


if __name__ == "__main__":
    main()

    
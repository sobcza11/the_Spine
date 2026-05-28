from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


RUN_TS = datetime.now(timezone.utc).isoformat()

INPUT_JSON = Path(
    "data/serving/oraclechambers/oc_geoscen_ingestion_bridge_v1.json"
)

OUTPUT_JSON = Path(
    "data/serving/oraclechambers/oc_local_intelligence_layer_v1.json"
)

OUTPUT_PARQUET = Path(
    "data/serving/oraclechambers/oc_local_intelligence_layer_v1.parquet"
)


def load_oc_bridge_payload() -> Dict[str, Any]:
    if not INPUT_JSON.exists():
        raise FileNotFoundError(
            f"Missing required input: {INPUT_JSON}"
        )

    with INPUT_JSON.open(
        "r",
        encoding="utf-8",
    ) as f:
        return json.load(f)


def build_active_panels(
    payload: Dict[str, Any]
) -> List[Dict[str, Any]]:

    panel_state = payload["panel_state"]
    visual_routing = payload["visual_routing"]

    priority_panel = visual_routing["priority_panel"]

    ordered_panels = [
        priority_panel,
        "regime_panel",
        "metric_panel",
        "historical_memory_panel",
        "rbl_panel",
    ]

    seen = set()

    active_panels: List[Dict[str, Any]] = []

    for panel_name in ordered_panels:

        if panel_name in seen:
            continue

        seen.add(panel_name)

        panel = panel_state.get(panel_name)

        if panel is None:
            continue

        active_panels.append(
            {
                "panel_name": panel_name,
                "priority": len(active_panels) + 1,
                "panel_payload": panel,
            }
        )

    return active_panels


def build_overlay_state(
    payload: Dict[str, Any]
) -> Dict[str, Any]:

    routing = payload["visual_routing"]

    return {
        "contradiction_overlay": routing[
            "show_contradiction_overlay"
        ],
        "historical_overlay": routing[
            "show_historical_overlay"
        ],
        "cb_divergence_overlay": routing[
            "show_cb_divergence_overlay"
        ],
        "regime_sequence_overlay": routing[
            "show_regime_sequence"
        ],
    }


def build_chart_state(
    payload: Dict[str, Any]
) -> Dict[str, Any]:

    routing = payload["visual_routing"]

    return {
        "primary_chart": routing["chart_routes"][
            "primary_chart"
        ],
        "secondary_chart": routing["chart_routes"][
            "secondary_chart"
        ],
        "tertiary_chart": routing["chart_routes"][
            "tertiary_chart"
        ],
        "adaptive_graph_routing": True,
        "evidence_linked_visualization": True,
    }


def build_runtime_state(
    payload: Dict[str, Any]
) -> Dict[str, Any]:

    routing = payload["routing"]

    return {
        "frontend_ready": routing[
            "frontend_ready"
        ],
        "local_runtime_ready": routing[
            "local_runtime_ready"
        ],
        "online_runtime_ready": routing[
            "online_runtime_ready"
        ],
        "display_mode": payload[
            "visual_routing"
        ]["display_mode"],
        "deployment_mode": "offline_first",
        "ai_dependency": routing[
            "ai_dependency"
        ],
    }


def build_executive_state(
    payload: Dict[str, Any]
) -> Dict[str, Any]:

    regime_panel = payload["panel_state"][
        "regime_panel"
    ]

    rbl_panel = payload["panel_state"][
        "rbl_panel"
    ]

    metric_panel = payload["panel_state"][
        "metric_panel"
    ]

    return {
        "headline_regime": regime_panel[
            "regime_label"
        ],
        "headline_confidence": regime_panel[
            "confidence"
        ],
        "headline_conviction": regime_panel[
            "conviction"
        ],
        "macro_temperature": regime_panel[
            "macro_temperature"
        ],
        "risk_posture": rbl_panel[
            "risk_posture"
        ],
        "decision_bias": rbl_panel[
            "decision_bias"
        ],
        "instability_risk": metric_panel[
            "instability_risk"
        ],
        "executive_summary": rbl_panel[
            "executive_summary"
        ],
    }


def build_oc_local_payload(
    bridge_payload: Dict[str, Any]
) -> Dict[str, Any]:

    active_panels = build_active_panels(
        bridge_payload
    )

    overlay_state = build_overlay_state(
        bridge_payload
    )

    chart_state = build_chart_state(
        bridge_payload
    )

    runtime_state = build_runtime_state(
        bridge_payload
    )

    executive_state = build_executive_state(
        bridge_payload
    )

    deployment_ready = bool(
        bridge_payload.get(
            "deployment_ready",
            False,
        )
        and runtime_state[
            "frontend_ready"
        ]
        and runtime_state[
            "local_runtime_ready"
        ]
    )

    return {
        "artifact": "oc_local_intelligence_layer_v1",
        "system": "OracleChambers",
        "layer": "OC Local Intelligence Layer",
        "version": "v1",
        "run_ts": RUN_TS,
        "deployment_ready": deployment_ready,
        "executive_state": executive_state,
        "active_panels": active_panels,
        "overlay_state": overlay_state,
        "chart_state": chart_state,
        "runtime_state": runtime_state,
        "routing": {
            "executive_ui_ready": True,
            "adaptive_graph_routing": True,
            "offline_runtime_ready": True,
            "online_runtime_ready": False,
            "ai_dependency": False,
        },
        "provenance": {
            "source_payload": str(INPUT_JSON),
            "source_artifact": bridge_payload[
                "artifact"
            ],
            "source_run_ts": bridge_payload[
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

    executive_state = payload[
        "executive_state"
    ]

    runtime_state = payload[
        "runtime_state"
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
        "headline_regime": executive_state[
            "headline_regime"
        ],
        "headline_confidence": executive_state[
            "headline_confidence"
        ],
        "headline_conviction": executive_state[
            "headline_conviction"
        ],
        "macro_temperature": executive_state[
            "macro_temperature"
        ],
        "risk_posture": executive_state[
            "risk_posture"
        ],
        "decision_bias": executive_state[
            "decision_bias"
        ],
        "display_mode": runtime_state[
            "display_mode"
        ],
        "deployment_mode": runtime_state[
            "deployment_mode"
        ],
        "offline_runtime_ready": payload[
            "routing"
        ]["offline_runtime_ready"],
        "online_runtime_ready": payload[
            "routing"
        ]["online_runtime_ready"],
        "ai_dependency": payload[
            "routing"
        ]["ai_dependency"],
    }

    pd.DataFrame([parquet_row]).to_parquet(
        OUTPUT_PARQUET,
        index=False,
    )


def main() -> None:

    bridge_payload = load_oc_bridge_payload()

    oc_payload = build_oc_local_payload(
        bridge_payload
    )

    write_outputs(oc_payload)

    runtime_state = oc_payload[
        "runtime_state"
    ]

    executive_state = oc_payload[
        "executive_state"
    ]

    print(
        "OC Local Intelligence Layer v1 complete"
    )

    print(
        f"deployment_ready: {oc_payload['deployment_ready']}"
    )

    print(
        f"headline_regime: {executive_state['headline_regime']}"
    )

    print(
        f"display_mode: {runtime_state['display_mode']}"
    )

    print(
        f"deployment_mode: {runtime_state['deployment_mode']}"
    )

    print(f"json_output: {OUTPUT_JSON}")

    print(f"parquet_output: {OUTPUT_PARQUET}")


if __name__ == "__main__":
    main()
    
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


RUN_TS = datetime.now(timezone.utc).isoformat()

INPUTS = {
    "manifest": Path(
        "data/serving/oraclechambers/oc_local_deployment_manifest_v1.json"
    ),
    "site_hydration": Path(
        "data/serving/oraclechambers/oc_local_site_hydration_v1.json"
    ),
    "runtime_controller": Path(
        "data/serving/oraclechambers/oc_runtime_controller_v1.json"
    ),
}

OUTPUT_JSON = Path(
    "data/serving/oraclechambers/oc_local_launch_controller_v1.json"
)

OUTPUT_PARQUET = Path(
    "data/serving/oraclechambers/oc_local_launch_controller_v1.parquet"
)


def load_json(
    path: Path
) -> Dict[str, Any]:

    if not path.exists():
        raise FileNotFoundError(
            f"Missing required input: {path}"
        )

    with path.open(
        "r",
        encoding="utf-8",
    ) as f:
        return json.load(f)


def load_inputs() -> Dict[str, Dict[str, Any]]:

    loaded: Dict[str, Dict[str, Any]] = {}

    for name, path in INPUTS.items():

        loaded[name] = load_json(path)

    return loaded


def build_boot_sequence(
    payloads: Dict[str, Dict[str, Any]]
) -> List[Dict[str, Any]]:

    manifest = payloads["manifest"]
    hydration = payloads["site_hydration"]
    runtime = payloads["runtime_controller"]

    return [
        {
            "step": 1,
            "stage": "manifest_validation",
            "deployment_ready": manifest[
                "deployment_ready"
            ],
            "status": "ready",
        },
        {
            "step": 2,
            "stage": "site_hydration",
            "deployment_ready": hydration[
                "deployment_ready"
            ],
            "status": "ready",
        },
        {
            "step": 3,
            "stage": "runtime_initialization",
            "deployment_ready": runtime[
                "deployment_ready"
            ],
            "status": "ready",
        },
        {
            "step": 4,
            "stage": "executive_dashboard_launch",
            "deployment_ready": True,
            "status": "ready",
        },
    ]


def build_runtime_launch_state(
    payloads: Dict[str, Dict[str, Any]]
) -> Dict[str, Any]:

    runtime = payloads[
        "runtime_controller"
    ]

    runtime_controls = runtime[
        "runtime_controls"
    ]

    return {
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
        "executive_monitoring_enabled": runtime_controls[
            "executive_monitoring_enabled"
        ],
    }


def build_dashboard_launch_state(
    payloads: Dict[str, Dict[str, Any]]
) -> Dict[str, Any]:

    hydration = payloads[
        "site_hydration"
    ]

    site = hydration[
        "site_payload"
    ]

    headline = site[
        "headline"
    ]

    return {
        "site_title": site[
            "site_title"
        ],
        "site_mode": site[
            "site_mode"
        ],
        "runtime_mode": site[
            "runtime_mode"
        ],
        "headline_regime": headline[
            "regime"
        ],
        "headline_confidence": headline[
            "confidence"
        ],
        "macro_temperature": headline[
            "macro_temperature"
        ],
        "risk_posture": headline[
            "risk_posture"
        ],
    }


def build_launch_payload() -> Dict[str, Any]:

    payloads = load_inputs()

    boot_sequence = build_boot_sequence(
        payloads
    )

    runtime_launch_state = (
        build_runtime_launch_state(
            payloads
        )
    )

    dashboard_launch_state = (
        build_dashboard_launch_state(
            payloads
        )
    )

    deployment_ready = all(
        step["deployment_ready"]
        for step in boot_sequence
    )

    return {
        "artifact": "oc_local_launch_controller_v1",
        "system": "OracleChambers",
        "layer": "OC Local Launch Controller",
        "version": "v1",
        "run_ts": RUN_TS,
        "deployment_ready": deployment_ready,
        "boot_sequence": boot_sequence,
        "runtime_launch_state": runtime_launch_state,
        "dashboard_launch_state": dashboard_launch_state,
        "routing": {
            "local_launch_ready": True,
            "offline_first": True,
            "online_runtime_ready": False,
            "executive_dashboard_ready": True,
            "ai_dependency": False,
        },
        "provenance": {
            "source_payload": "oraclechambers_runtime_stack",
            "source_artifact": "oc_runtime_stack_bundle",
            "source_run_ts": RUN_TS,
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

    runtime = payload[
        "runtime_launch_state"
    ]

    dashboard = payload[
        "dashboard_launch_state"
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
        "runtime_mode": runtime[
            "runtime_mode"
        ],
        "refresh_interval_seconds": runtime[
            "refresh_interval_seconds"
        ],
        "offline_first": runtime[
            "offline_first"
        ],
        "headline_regime": dashboard[
            "headline_regime"
        ],
        "headline_confidence": dashboard[
            "headline_confidence"
        ],
        "macro_temperature": dashboard[
            "macro_temperature"
        ],
        "risk_posture": dashboard[
            "risk_posture"
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

    launch_payload = build_launch_payload()

    write_outputs(
        launch_payload
    )

    dashboard = launch_payload[
        "dashboard_launch_state"
    ]

    print(
        "OC Local Launch Controller v1 complete"
    )

    print(
        f"deployment_ready: {launch_payload['deployment_ready']}"
    )

    print(
        f"headline_regime: {dashboard['headline_regime']}"
    )

    print(
        f"headline_confidence: {dashboard['headline_confidence']:.2f}"
    )

    print(f"json_output: {OUTPUT_JSON}")

    print(f"parquet_output: {OUTPUT_PARQUET}")


if __name__ == "__main__":
    main()
    
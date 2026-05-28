from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


RUN_TS = datetime.now(timezone.utc).isoformat()

INPUTS = {
    "geoscen_synthesis": Path(
        "data/serving/geoscen/geoscen_institutional_synthesis_v1.json"
    ),
    "oc_ingestion_bridge": Path(
        "data/serving/oraclechambers/oc_geoscen_ingestion_bridge_v1.json"
    ),
    "oc_local_intelligence": Path(
        "data/serving/oraclechambers/oc_local_intelligence_layer_v1.json"
    ),
    "oc_runtime_controller": Path(
        "data/serving/oraclechambers/oc_runtime_controller_v1.json"
    ),
    "oc_adaptive_layout": Path(
        "data/serving/oraclechambers/oc_adaptive_layout_engine_v1.json"
    ),
    "oc_executive_dashboard": Path(
        "data/serving/oraclechambers/oc_executive_dashboard_state_v1.json"
    ),
}

OUTPUT_JSON = Path(
    "data/serving/oraclechambers/oc_local_deployment_manifest_v1.json"
)

OUTPUT_PARQUET = Path(
    "data/serving/oraclechambers/oc_local_deployment_manifest_v1.parquet"
)


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(
            f"Missing required input: {path}"
        )

    with path.open(
        "r",
        encoding="utf-8",
    ) as f:
        return json.load(f)


def build_asset_registry() -> Dict[str, Any]:

    registry: Dict[str, Any] = {}

    for asset_name, path in INPUTS.items():

        payload = load_json(path)

        registry[asset_name] = {
            "artifact": payload.get("artifact"),
            "system": payload.get("system"),
            "layer": payload.get("layer"),
            "version": payload.get("version"),
            "run_ts": payload.get("run_ts"),
            "deployment_ready": payload.get(
                "deployment_ready",
                False,
            ),
            "path": str(path),
            "routing": payload.get("routing", {}),
            "provenance": payload.get("provenance", {}),
        }

    return registry


def build_runtime_manifest(
    registry: Dict[str, Any]
) -> Dict[str, Any]:

    dashboard = registry[
        "oc_executive_dashboard"
    ]

    runtime = registry[
        "oc_runtime_controller"
    ]

    return {
        "deployment_mode": "offline_first",
        "local_runtime_enabled": True,
        "online_runtime_enabled": False,
        "executive_dashboard_ready": dashboard[
            "deployment_ready"
        ],
        "runtime_controller_ready": runtime[
            "deployment_ready"
        ],
        "ai_dependency": False,
        "serving_root": "data/serving/oraclechambers",
        "primary_dashboard_artifact": dashboard[
            "artifact"
        ],
        "primary_runtime_artifact": runtime[
            "artifact"
        ],
    }


def build_validation_summary(
    registry: Dict[str, Any]
) -> Dict[str, Any]:

    total_assets = len(registry)

    ready_assets = sum(
        1
        for asset in registry.values()
        if asset["deployment_ready"]
    )

    ready_ratio = (
        ready_assets / total_assets
        if total_assets
        else 0.0
    )

    return {
        "total_assets": total_assets,
        "ready_assets": ready_assets,
        "ready_ratio": round(
            ready_ratio,
            4,
        ),
        "all_assets_ready": ready_assets
        == total_assets,
    }


def build_manifest_payload() -> Dict[str, Any]:

    registry = build_asset_registry()

    runtime_manifest = build_runtime_manifest(
        registry
    )

    validation_summary = build_validation_summary(
        registry
    )

    deployment_ready = bool(
        validation_summary["all_assets_ready"]
        and runtime_manifest[
            "local_runtime_enabled"
        ]
        and not runtime_manifest[
            "online_runtime_enabled"
        ]
    )

    return {
        "artifact": "oc_local_deployment_manifest_v1",
        "system": "OracleChambers",
        "layer": "OC Local Deployment Manifest",
        "version": "v1",
        "run_ts": RUN_TS,
        "deployment_ready": deployment_ready,
        "asset_registry": registry,
        "runtime_manifest": runtime_manifest,
        "validation_summary": validation_summary,
        "routing": {
            "local_deployment_ready": deployment_ready,
            "offline_first": True,
            "online_runtime_ready": False,
            "executive_ui_ready": True,
            "adaptive_layout_ready": True,
            "runtime_controller_ready": True,
            "ai_dependency": False,
        },
        "provenance": {
            "source_payload": "oraclechambers_local_stack_assets",
            "source_artifact": "oc_local_stack_bundle",
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

    validation = payload[
        "validation_summary"
    ]

    runtime = payload[
        "runtime_manifest"
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
        "deployment_mode": runtime[
            "deployment_mode"
        ],
        "local_runtime_enabled": runtime[
            "local_runtime_enabled"
        ],
        "online_runtime_enabled": runtime[
            "online_runtime_enabled"
        ],
        "total_assets": validation[
            "total_assets"
        ],
        "ready_assets": validation[
            "ready_assets"
        ],
        "ready_ratio": validation[
            "ready_ratio"
        ],
        "all_assets_ready": validation[
            "all_assets_ready"
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

    manifest_payload = build_manifest_payload()

    write_outputs(
        manifest_payload
    )

    validation = manifest_payload[
        "validation_summary"
    ]

    print(
        "OC Local Deployment Manifest v1 complete"
    )

    print(
        f"deployment_ready: {manifest_payload['deployment_ready']}"
    )

    print(
        f"ready_assets: {validation['ready_assets']}/{validation['total_assets']}"
    )

    print(
        f"ready_ratio: {validation['ready_ratio']:.2f}"
    )

    print(f"json_output: {OUTPUT_JSON}")

    print(f"parquet_output: {OUTPUT_PARQUET}")


if __name__ == "__main__":
    main()

    
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import pandas as pd


RUN_TS = datetime.now(timezone.utc).isoformat()

INPUT_JSON = Path(
    "data/serving/oraclechambers/oc_receiving_area_registry_v1.json"
)

OUTPUT_JSON = Path(
    "data/serving/oraclechambers/oc_cross_domain_overlay_router_v1.json"
)

OUTPUT_PARQUET = Path(
    "data/serving/oraclechambers/oc_cross_domain_overlay_router_v1.parquet"
)


def load_registry() -> Dict[str, Any]:

    with INPUT_JSON.open(
        "r",
        encoding="utf-8",
    ) as f:

        return json.load(f)


def build_payload() -> Dict[str, Any]:

    registry = load_registry()

    overlays = registry[
        "cross_domain_overlay_layers"
    ]

    return {
        "artifact": (
            "oc_cross_domain_overlay_router_v1"
        ),
        "system": "OracleChambers",
        "layer": (
            "OC Cross Domain Overlay Router"
        ),
        "version": "v1",
        "run_ts": RUN_TS,
        "deployment_ready": True,
        "overlay_routes": overlays,
        "routing": {
            "overlay_routing_ready": True,
            "cross_asset_ready": True,
            "offline_first": True,
            "online_ready": False,
            "ai_dependency": False,
        },
        "provenance": {
            "source_payload": str(
                INPUT_JSON
            ),
            "source_artifact": registry[
                "artifact"
            ],
            "source_run_ts": registry[
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

    pd.DataFrame(
        payload["overlay_routes"]
    ).to_parquet(
        OUTPUT_PARQUET,
        index=False,
    )


def main() -> None:

    payload = build_payload()

    write_outputs(payload)

    print(
        "OC Cross Domain Overlay Router v1 complete"
    )

    print(
        f"deployment_ready: {payload['deployment_ready']}"
    )

    print(
        f"overlay_routes: {len(payload['overlay_routes'])}"
    )

    print(f"json_output: {OUTPUT_JSON}")

    print(f"parquet_output: {OUTPUT_PARQUET}")


if __name__ == "__main__":
    main()

    
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


RUN_TS = datetime.now(timezone.utc).isoformat()

INPUT_JSON = Path(
    "data/serving/oraclechambers/oc_receiving_area_registry_v1.json"
)

OUTPUT_JSON = Path(
    "data/serving/oraclechambers/oc_compartment_runtime_mapper_v1.json"
)

OUTPUT_PARQUET = Path(
    "data/serving/oraclechambers/oc_compartment_runtime_mapper_v1.parquet"
)


def load_registry() -> Dict[str, Any]:

    if not INPUT_JSON.exists():
        raise FileNotFoundError(
            f"Missing registry: {INPUT_JSON}"
        )

    with INPUT_JSON.open(
        "r",
        encoding="utf-8",
    ) as f:

        return json.load(f)


def build_runtime_map(
    registry: Dict[str, Any]
) -> List[Dict[str, Any]]:

    rows = []

    for domain in registry[
        "domain_registry"
    ]:

        domain_name = domain[
            "domain"
        ]

        for compartment in domain[
            "compartments"
        ]:

            rows.append(
                {
                    "domain": domain_name,
                    "compartment": compartment[
                        "compartment"
                    ],
                    "route": compartment[
                        "route"
                    ],
                    "runtime_state": "offline_ready",
                    "render_priority": compartment[
                        "order"
                    ],
                    "online_ready": False,
                    "adaptive_enabled": True,
                    "routing_enabled": True,
                }
            )

    return rows


def build_payload() -> Dict[str, Any]:

    registry = load_registry()

    runtime_map = build_runtime_map(
        registry
    )

    return {
        "artifact": (
            "oc_compartment_runtime_mapper_v1"
        ),
        "system": "OracleChambers",
        "layer": (
            "OC Compartment Runtime Mapper"
        ),
        "version": "v1",
        "run_ts": RUN_TS,
        "deployment_ready": True,
        "runtime_map": runtime_map,
        "routing": {
            "offline_first": True,
            "runtime_mapping_ready": True,
            "online_ready": False,
            "adaptive_runtime_ready": True,
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
        payload["runtime_map"]
    ).to_parquet(
        OUTPUT_PARQUET,
        index=False,
    )


def main() -> None:

    payload = build_payload()

    write_outputs(payload)

    print(
        "OC Compartment Runtime Mapper v1 complete"
    )

    print(
        f"deployment_ready: {payload['deployment_ready']}"
    )

    print(
        f"runtime_routes: {len(payload['runtime_map'])}"
    )

    print(f"json_output: {OUTPUT_JSON}")

    print(f"parquet_output: {OUTPUT_PARQUET}")


if __name__ == "__main__":
    main()

    
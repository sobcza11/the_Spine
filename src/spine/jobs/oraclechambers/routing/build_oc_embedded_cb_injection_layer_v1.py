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
    "data/serving/oraclechambers/oc_embedded_cb_injection_layer_v1.json"
)

OUTPUT_PARQUET = Path(
    "data/serving/oraclechambers/oc_embedded_cb_injection_layer_v1.parquet"
)


def load_registry() -> Dict[str, Any]:

    with INPUT_JSON.open(
        "r",
        encoding="utf-8",
    ) as f:

        return json.load(f)


def build_payload() -> Dict[str, Any]:

    registry = load_registry()

    embedded_layers = registry[
        "embedded_layers"
    ]

    return {
        "artifact": (
            "oc_embedded_cb_injection_layer_v1"
        ),
        "system": "OracleChambers",
        "layer": (
            "OC Embedded CB Injection Layer"
        ),
        "version": "v1",
        "run_ts": RUN_TS,
        "deployment_ready": True,
        "embedded_layers": embedded_layers,
        "routing": {
            "cb_injection_ready": True,
            "embedded_cognition_ready": True,
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
        payload["embedded_layers"]
    ).to_parquet(
        OUTPUT_PARQUET,
        index=False,
    )


def main() -> None:

    payload = build_payload()

    write_outputs(payload)

    print(
        "OC Embedded CB Injection Layer v1 complete"
    )

    print(
        f"deployment_ready: {payload['deployment_ready']}"
    )

    print(
        f"embedded_layers: {len(payload['embedded_layers'])}"
    )

    print(f"json_output: {OUTPUT_JSON}")

    print(f"parquet_output: {OUTPUT_PARQUET}")


if __name__ == "__main__":
    main()

    
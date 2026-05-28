from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import pandas as pd


RUN_TS = datetime.now(timezone.utc).isoformat()

OUTPUT_JSON = Path(
    "data/serving/oraclechambers/oc_live_runtime_sync_layer_v1.json"
)

OUTPUT_PARQUET = Path(
    "data/serving/oraclechambers/oc_live_runtime_sync_layer_v1.parquet"
)


def build_sync_registry() -> List[Dict]:

    return [
        {
            "domain": "FX",
            "sync_enabled": False,
            "sync_interval_seconds": 60,
        },
        {
            "domain": "RATES",
            "sync_enabled": False,
            "sync_interval_seconds": 60,
        },
        {
            "domain": "C_FLOW",
            "sync_enabled": False,
            "sync_interval_seconds": 120,
        },
        {
            "domain": "EQUITIES_INDEX",
            "sync_enabled": False,
            "sync_interval_seconds": 30,
        },
        {
            "domain": "EQUITIES_SECTOR",
            "sync_enabled": False,
            "sync_interval_seconds": 60,
        },
    ]


def build_payload() -> Dict:

    registry = build_sync_registry()

    return {
        "artifact": (
            "oc_live_runtime_sync_layer_v1"
        ),
        "system": "OracleChambers",
        "layer": (
            "OC Live Runtime Sync Layer"
        ),
        "version": "v1",
        "run_ts": RUN_TS,
        "deployment_ready": False,
        "sync_registry": registry,
        "routing": {
            "runtime_sync_ready": True,
            "offline_first": True,
            "live_runtime_ready": False,
            "ai_dependency": False,
        },
    }


def write_outputs(payload: Dict) -> None:

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
        payload["sync_registry"]
    ).to_parquet(
        OUTPUT_PARQUET,
        index=False,
    )


def main() -> None:

    payload = build_payload()

    write_outputs(payload)

    print(
        "OC Live Runtime Sync Layer v1 complete"
    )

    print(
        f"sync_domains: {len(payload['sync_registry'])}"
    )

    print(f"json_output: {OUTPUT_JSON}")

    print(f"parquet_output: {OUTPUT_PARQUET}")


if __name__ == "__main__":
    main()

    
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import pandas as pd


RUN_TS = datetime.now(timezone.utc).isoformat()

OUTPUT_JSON = Path(
    "data/serving/oraclechambers/oc_websocket_event_router_v1.json"
)

OUTPUT_PARQUET = Path(
    "data/serving/oraclechambers/oc_websocket_event_router_v1.parquet"
)


def build_event_routes() -> List[Dict]:

    return [
        {
            "event": "FX_UPDATE",
            "route": "RBL | FX",
            "websocket_ready": False,
        },
        {
            "event": "RATES_UPDATE",
            "route": "RBL | RATES",
            "websocket_ready": False,
        },
        {
            "event": "C_FLOW_UPDATE",
            "route": "RBL | C_FLOW",
            "websocket_ready": False,
        },
        {
            "event": "EQUITIES_INDEX_UPDATE",
            "route": "RBL | EQUITIES_INDEX",
            "websocket_ready": False,
        },
        {
            "event": "EQUITIES_SECTOR_UPDATE",
            "route": "RBL | EQUITIES_SECTOR",
            "websocket_ready": False,
        },
    ]


def build_payload() -> Dict:

    routes = build_event_routes()

    return {
        "artifact": (
            "oc_websocket_event_router_v1"
        ),
        "system": "OracleChambers",
        "layer": (
            "OC Websocket Event Router"
        ),
        "version": "v1",
        "run_ts": RUN_TS,
        "deployment_ready": False,
        "event_routes": routes,
        "routing": {
            "websocket_routing_ready": True,
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
        payload["event_routes"]
    ).to_parquet(
        OUTPUT_PARQUET,
        index=False,
    )


def main() -> None:

    payload = build_payload()

    write_outputs(payload)

    print(
        "OC Websocket Event Router v1 complete"
    )

    print(
        f"routes: {len(payload['event_routes'])}"
    )

    print(f"json_output: {OUTPUT_JSON}")

    print(f"parquet_output: {OUTPUT_PARQUET}")


if __name__ == "__main__":
    main()
    
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import pandas as pd


RUN_TS = datetime.now(timezone.utc).isoformat()

OUTPUT_JSON = Path(
    "data/serving/oraclechambers/oc_online_source_registry_skeleton_v1.json"
)

OUTPUT_PARQUET = Path(
    "data/serving/oraclechambers/oc_online_source_registry_skeleton_v1.parquet"
)


def build_registry() -> List[Dict]:

    return [
        {
            "domain": "FX",
            "source_group": "market_data",
            "online_ready": False,
            "planned_sources": [
                "FRED",
                "Polygon",
                "Bloomberg",
            ],
        },
        {
            "domain": "RATES",
            "source_group": "rates_data",
            "online_ready": False,
            "planned_sources": [
                "FRED",
                "Treasury",
                "Bloomberg",
            ],
        },
        {
            "domain": "C_FLOW",
            "source_group": "commodity_data",
            "online_ready": False,
            "planned_sources": [
                "EIA",
                "FRED",
                "Commodity APIs",
            ],
        },
        {
            "domain": "EQUITIES_INDEX",
            "source_group": "equity_index_data",
            "online_ready": False,
            "planned_sources": [
                "Polygon",
                "YahooFinance",
                "Bloomberg",
            ],
        },
        {
            "domain": "EQUITIES_SECTOR",
            "source_group": "sector_data",
            "online_ready": False,
            "planned_sources": [
                "ETF APIs",
                "Polygon",
                "Bloomberg",
            ],
        },
    ]


def build_payload() -> Dict:

    registry = build_registry()

    return {
        "artifact": (
            "oc_online_source_registry_skeleton_v1"
        ),
        "system": "OracleChambers",
        "layer": (
            "OC Online Source Registry Skeleton"
        ),
        "version": "v1",
        "run_ts": RUN_TS,
        "deployment_ready": False,
        "source_registry": registry,
        "routing": {
            "online_source_registry_ready": True,
            "offline_first": True,
            "online_execution_ready": False,
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
        payload["source_registry"]
    ).to_parquet(
        OUTPUT_PARQUET,
        index=False,
    )


def main() -> None:

    payload = build_payload()

    write_outputs(payload)

    print(
        "OC Online Source Registry Skeleton v1 complete"
    )

    print(
        f"domains: {len(payload['source_registry'])}"
    )

    print(f"json_output: {OUTPUT_JSON}")

    print(f"parquet_output: {OUTPUT_PARQUET}")


if __name__ == "__main__":
    main()

    
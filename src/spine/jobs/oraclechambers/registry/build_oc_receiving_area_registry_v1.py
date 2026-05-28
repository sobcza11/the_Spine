from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


RUN_TS = datetime.now(timezone.utc).isoformat()

OUTPUT_JSON = Path(
    "data/serving/oraclechambers/oc_receiving_area_registry_v1.json"
)

OUTPUT_PARQUET = Path(
    "data/serving/oraclechambers/oc_receiving_area_registry_v1.parquet"
)


DOMAIN_BUILD_ORDER = [
    "FX",
    "RATES",
    "C_FLOW",
    "EQUITIES_INDEX",
    "EQUITIES_SECTOR",
    "CROSS_DOMAIN_OVERLAY",
    "EMBEDDED_WITHIN_ALL_DOMAINS",
]


COMPARTMENT_ORDER = [
    "RBL",
    "ZT",
    "METRIC",
    "NLP",
]


def build_primary_domains() -> List[Dict[str, Any]]:

    domains = [
        {
            "domain": "FX",
            "priority": 1,
            "description": (
                "Global liquidity, currency stress, "
                "carry structure & dollar pressure"
            ),
        },
        {
            "domain": "RATES",
            "priority": 2,
            "description": (
                "Monetary regime, duration cognition "
                "& yield curve structure"
            ),
        },
        {
            "domain": "C_FLOW",
            "priority": 3,
            "description": (
                "Commodity flow, inflation pressure "
                "& real economy stress"
            ),
        },
        {
            "domain": "EQUITIES_INDEX",
            "priority": 4,
            "description": (
                "Broad beta posture & systemic "
                "market cognition"
            ),
        },
        {
            "domain": "EQUITIES_SECTOR",
            "priority": 5,
            "description": (
                "Sector rotation, internal breadth "
                "& leadership structure"
            ),
        },
    ]

    return domains


def build_domain_compartments(
    domain_name: str
) -> List[Dict[str, Any]]:

    return [
        {
            "compartment": "RBL",
            "route": f"RBL | {domain_name}",
            "order": 1,
            "purpose": (
                "Interpretation layer"
            ),
            "status": "active_build_target",
        },
        {
            "compartment": "ZT",
            "route": f"Zₜ | {domain_name}",
            "order": 2,
            "purpose": (
                "Zeitgeist cognition layer"
            ),
            "status": "active_build_target",
        },
        {
            "compartment": "METRIC",
            "route": f"{domain_name}_METRIC",
            "order": 3,
            "purpose": (
                "Metric & scoring layer"
            ),
            "status": "reserved",
        },
        {
            "compartment": "NLP",
            "route": f"GeoScen_NLP | {domain_name}",
            "order": 4,
            "purpose": (
                "Narrative ingestion layer"
            ),
            "status": "reserved",
        },
    ]


def build_overlay_layers() -> List[Dict[str, Any]]:

    return [
        {
            "route": "RBL | CONTRADICTION",
            "purpose": (
                "Cross-asset disagreement engine"
            ),
            "layer_type": "overlay",
            "priority": 6,
        },
        {
            "route": "RBL | HISTORICAL_MEMORY",
            "purpose": (
                "Historical analog engine"
            ),
            "layer_type": "overlay",
            "priority": 7,
        },
        {
            "route": "RBL | FINAL_METRIC",
            "purpose": (
                "Executive synthesis engine"
            ),
            "layer_type": "overlay",
            "priority": 8,
        },
    ]


def build_embedded_layers() -> List[Dict[str, Any]]:

    return [
        {
            "route": "RBL | CB",
            "purpose": (
                "Embedded central bank cognition"
            ),
            "layer_type": "embedded",
            "embedded_domains": [
                "FX",
                "RATES",
                "C_FLOW",
                "EQUITIES_INDEX",
                "EQUITIES_SECTOR",
            ],
        }
    ]


def build_registry_payload() -> Dict[str, Any]:

    primary_domains = (
        build_primary_domains()
    )

    domain_registry = []

    for domain in primary_domains:

        domain_registry.append(
            {
                "domain": domain[
                    "domain"
                ],
                "priority": domain[
                    "priority"
                ],
                "description": domain[
                    "description"
                ],
                "compartments": (
                    build_domain_compartments(
                        domain["domain"]
                    )
                ),
            }
        )

    overlay_layers = (
        build_overlay_layers()
    )

    embedded_layers = (
        build_embedded_layers()
    )

    return {
        "artifact": (
            "oc_receiving_area_registry_v1"
        ),
        "system": "OracleChambers",
        "layer": (
            "OC Receiving Area Registry"
        ),
        "version": "v1",
        "run_ts": RUN_TS,
        "deployment_ready": True,
        "domain_build_order": (
            DOMAIN_BUILD_ORDER
        ),
        "compartment_order": (
            COMPARTMENT_ORDER
        ),
        "domain_registry": (
            domain_registry
        ),
        "cross_domain_overlay_layers": (
            overlay_layers
        ),
        "embedded_layers": (
            embedded_layers
        ),
        "routing": {
            "offline_first": True,
            "online_ready": False,
            "adaptive_routing_ready": True,
            "institutional_registry_ready": True,
            "ai_dependency": False,
        },
        "provenance": {
            "source_payload": (
                "oraclechambers_receiving_area_design"
            ),
            "source_artifact": (
                "oc_receiving_area_registry_v1"
            ),
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

    rows = []

    for domain in payload[
        "domain_registry"
    ]:

        for compartment in domain[
            "compartments"
        ]:

            rows.append(
                {
                    "domain": domain[
                        "domain"
                    ],
                    "domain_priority": domain[
                        "priority"
                    ],
                    "compartment": compartment[
                        "compartment"
                    ],
                    "route": compartment[
                        "route"
                    ],
                    "compartment_order": compartment[
                        "order"
                    ],
                    "purpose": compartment[
                        "purpose"
                    ],
                    "status": compartment[
                        "status"
                    ],
                }
            )

    pd.DataFrame(rows).to_parquet(
        OUTPUT_PARQUET,
        index=False,
    )


def main() -> None:

    payload = (
        build_registry_payload()
    )

    write_outputs(payload)

    print(
        "OC Receiving Area Registry v1 complete"
    )

    print(
        f"deployment_ready: {payload['deployment_ready']}"
    )

    print(
        f"domain_count: {len(payload['domain_registry'])}"
    )

    print(
        f"overlay_count: {len(payload['cross_domain_overlay_layers'])}"
    )

    print(f"json_output: {OUTPUT_JSON}")

    print(f"parquet_output: {OUTPUT_PARQUET}")


if __name__ == "__main__":
    main()
    
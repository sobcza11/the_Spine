from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


APPROVED_CONNECTORS = [
    {
        "name": "Tiingo",
        "category": "market_data",
        "status": "approved",
        "online_enabled": False,
    },
    {
        "name": "FRED",
        "category": "macro_data",
        "status": "approved",
        "online_enabled": False,
    },
    {
        "name": "Treasury",
        "category": "rates_data",
        "status": "approved",
        "online_enabled": False,
    },
    {
        "name": "Polygon",
        "category": "market_data",
        "status": "approved",
        "online_enabled": False,
    },
    {
        "name": "EIA",
        "category": "energy_data",
        "status": "approved",
        "online_enabled": False,
    },
    {
        "name": "WRDS",
        "category": "institutional_data",
        "status": "approved",
        "online_enabled": False,
    },
    {
        "name": "NBIS",
        "category": "rates_data",
        "status": "approved",
        "online_enabled": False,
    },
]


def build_approved_data_connector_registry_v1() -> dict[str, Any]:
    return {
        "artifact": "oc_approved_data_connector_registry_v1",
        "layer": "OracleChambers Approved Data Connector Registry",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "connector_registry_ready": True,
        "online_transition_allowed": False,
        "approved_connectors": APPROVED_CONNECTORS,
        "blocked_sources": [
            "Yahoo Finance",
            "Unverified scraping endpoints",
            "Anonymous APIs",
        ],
    }


if __name__ == "__main__":
    print(build_approved_data_connector_registry_v1())

    
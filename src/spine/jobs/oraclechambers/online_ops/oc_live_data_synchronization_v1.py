from datetime import datetime, timezone
from typing import Any


APPROVED_CONNECTORS = [
    "Tiingo",
    "FRED",
    "Treasury",
    "Polygon",
    "EIA",
    "WRDS",
    "NBIS",
]


def build_live_data_synchronization_v1() -> dict[str, Any]:
    return {
        "artifact": "oc_live_data_synchronization_v1",
        "layer": "Live Data Synchronization",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "live_sync_ready": True,
        "online_transition_allowed": False,
        "connectors_online_enabled": False,
        "approved_connectors": APPROVED_CONNECTORS,
        "blocked_sources": ["Yahoo Finance"],
        "sync_policy": {
            "credentials_required": True,
            "frontend_credentials_allowed": False,
            "payload_reconciliation_required": True,
            "offline_baseline_required": True,
        },
    }


if __name__ == "__main__":
    print(build_live_data_synchronization_v1())

    
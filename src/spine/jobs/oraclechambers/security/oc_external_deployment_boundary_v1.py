from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def build_external_deployment_boundary_v1() -> dict[str, Any]:
    return {
        "artifact": "oc_external_deployment_boundary_v1",
        "layer": "OracleChambers External Deployment Boundary",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "external_boundary_ready": True,
        "online_transition_allowed": False,
        "allowed_local_origins": [
            "http://127.0.0.1",
            "http://localhost",
        ],
        "blocked_external_exposure": True,
        "deployment_tiers": {
            "offline_local": {
                "allowed": True,
                "description": "Segmented offline/local runtime.",
            },
            "hybrid_staging": {
                "allowed": False,
                "description": "Future controlled online mirror staging.",
            },
            "institutional_online": {
                "allowed": False,
                "description": "Future enterprise deployment tier.",
            },
        },
    }


if __name__ == "__main__":
    print(build_external_deployment_boundary_v1())

    
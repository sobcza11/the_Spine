from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


ROLES = {
    "executive_viewer": {
        "can_view": True,
        "can_mutate_runtime": False,
        "can_approve_deployment": False,
    },
    "analyst_operator": {
        "can_view": True,
        "can_mutate_runtime": False,
        "can_approve_deployment": False,
    },
    "runtime_operator": {
        "can_view": True,
        "can_mutate_runtime": True,
        "can_approve_deployment": False,
    },
    "deployment_approver": {
        "can_view": True,
        "can_mutate_runtime": False,
        "can_approve_deployment": True,
    },
}


def build_identity_access_governance_v1() -> dict[str, Any]:
    return {
        "artifact": "oc_identity_access_governance_v1",
        "layer": "OracleChambers Identity & Access Governance",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "identity_governance_ready": True,
        "default_access": "deny",
        "roles": ROLES,
    }


if __name__ == "__main__":
    print(build_identity_access_governance_v1())

    
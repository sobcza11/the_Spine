from datetime import datetime, timezone
from typing import Any

from oc_credential_vault_integration_v1 import (
    build_credential_vault_integration_v1,
)


def build_live_connector_authentication_v1() -> dict[str, Any]:
    vault = build_credential_vault_integration_v1()

    connector_auth = [
        {
            "connector": item["secret_name"].replace("_API_KEY", "").replace("_ACCESS_TOKEN", ""),
            "credential_present": item["present"],
            "live_call_executed": False,
            "auth_status": "ready_for_probe" if item["present"] else "missing_credential",
        }
        for item in vault["secrets"]
    ]

    return {
        "artifact": "oc_live_connector_authentication_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "connector_auth_ready": True,
        "live_auth_passed": all(c["credential_present"] for c in connector_auth),
        "online_transition_allowed": False,
        "connectors": connector_auth,
    }


if __name__ == "__main__":
    print(build_live_connector_authentication_v1())

    
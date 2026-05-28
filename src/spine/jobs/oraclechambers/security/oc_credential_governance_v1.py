from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def build_credential_governance_v1() -> dict[str, Any]:
    return {
        "artifact": "oc_credential_governance_v1",
        "layer": "OracleChambers Credential Governance",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "credential_governance_ready": True,
        "frontend_secrets_allowed": False,
        "approved_secret_locations": [
            "environment_variables",
            "local_secret_store",
            "future_institutional_vault",
        ],
        "banned_locations": [
            "frontend_code",
            "static_json_payloads",
            "public_offline_sites",
            "committed_source_files",
        ],
    }


if __name__ == "__main__":
    print(build_credential_governance_v1())

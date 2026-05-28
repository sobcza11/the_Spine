from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class SecurityControl:
    name: str
    required: bool
    implemented: bool
    description: str


def build_deployment_security_contract_v1() -> dict[str, Any]:
    controls = [
        SecurityControl(
            name="local_only_default",
            required=True,
            implemented=True,
            description="Runtime defaults to local-only operation during productization.",
        ),
        SecurityControl(
            name="api_authentication",
            required=True,
            implemented=False,
            description="Authentication required before external runtime exposure.",
        ),
        SecurityControl(
            name="rbac",
            required=True,
            implemented=False,
            description="Role-based access control required for institutional deployment.",
        ),
        SecurityControl(
            name="credential_governance",
            required=True,
            implemented=False,
            description="API keys and source credentials must never live in frontend code.",
        ),
        SecurityControl(
            name="cors_boundary",
            required=True,
            implemented=True,
            description="FastAPI CORS boundary restricted to local frontend origins.",
        ),
    ]

    return {
        "artifact": "oc_deployment_security_contract_v1",
        "layer": "OracleChambers Deployment Security",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "external_deployment_ready": False,
        "security_ready_for_local_runtime": True,
        "controls": [control.__dict__ for control in controls],
    }


if __name__ == "__main__":
    output = build_deployment_security_contract_v1()

    for key, value in output.items():
        print(f"{key}: {value}")

        
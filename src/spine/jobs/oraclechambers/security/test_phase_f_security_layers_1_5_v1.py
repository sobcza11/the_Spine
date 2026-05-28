from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from oc_identity_access_governance_v1 import build_identity_access_governance_v1
from oc_credential_governance_v1 import build_credential_governance_v1
from oc_runtime_audit_ledger_v1 import build_runtime_audit_ledger_v1
from oc_governance_enforcement_engine_v1 import build_governance_enforcement_engine_v1
from oc_external_deployment_boundary_v1 import build_external_deployment_boundary_v1


def main() -> None:
    identity = build_identity_access_governance_v1()
    credentials = build_credential_governance_v1()
    audit = build_runtime_audit_ledger_v1()
    enforcement = build_governance_enforcement_engine_v1()
    boundary = build_external_deployment_boundary_v1()

    failures: list[str] = []

    if not identity.get("identity_governance_ready"):
        failures.append("identity_governance_not_ready")

    if not credentials.get("credential_governance_ready"):
        failures.append("credential_governance_not_ready")

    if credentials.get("frontend_secrets_allowed"):
        failures.append("frontend_secrets_should_be_blocked")

    if not audit.get("audit_ledger_ready"):
        failures.append("audit_ledger_not_ready")

    if not enforcement.get("governance_enforcement_ready"):
        failures.append("governance_enforcement_not_ready")

    if not boundary.get("external_boundary_ready"):
        failures.append("external_boundary_not_ready")

    if boundary.get("online_transition_allowed"):
        failures.append("online_transition_should_be_blocked")

    checks = enforcement.get("checks", [])

    prohibited_allowed = [
        check for check in checks
        if check["action"] in {
            "open_online_transition_without_approval",
            "store_secret_in_frontend",
        }
        and check["allowed"]
    ]

    if prohibited_allowed:
        failures.append("prohibited_actions_allowed")

    result: dict[str, Any] = {
        "artifact": "test_phase_f_security_layers_1_5_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "passed": len(failures) == 0,
        "failed_checks": failures,
        "validated_layers": [
            identity["artifact"],
            credentials["artifact"],
            audit["artifact"],
            enforcement["artifact"],
            boundary["artifact"],
        ],
    }

    print(result)

    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
    
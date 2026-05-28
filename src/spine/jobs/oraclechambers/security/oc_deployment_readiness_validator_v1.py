from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from oc_identity_access_governance_v1 import build_identity_access_governance_v1
from oc_credential_governance_v1 import build_credential_governance_v1
from oc_runtime_audit_ledger_v1 import build_runtime_audit_ledger_v1
from oc_governance_enforcement_engine_v1 import build_governance_enforcement_engine_v1
from oc_external_deployment_boundary_v1 import build_external_deployment_boundary_v1
from oc_institutional_security_monitoring_v1 import build_institutional_security_monitoring_v1
from oc_executive_approval_workflow_v1 import build_executive_approval_workflow_v1
from oc_runtime_kill_switch_v1 import build_runtime_kill_switch_v1
from oc_compliance_provenance_layer_v1 import build_compliance_provenance_layer_v1


def build_deployment_readiness_validator_v1() -> dict[str, Any]:
    identity = build_identity_access_governance_v1()
    credentials = build_credential_governance_v1()
    audit = build_runtime_audit_ledger_v1()
    enforcement = build_governance_enforcement_engine_v1()
    boundary = build_external_deployment_boundary_v1()
    monitoring = build_institutional_security_monitoring_v1()
    approval = build_executive_approval_workflow_v1()
    kill_switch = build_runtime_kill_switch_v1()
    provenance = build_compliance_provenance_layer_v1()

    checks = {
        "identity_governance_ready": identity.get("identity_governance_ready"),
        "credential_governance_ready": credentials.get("credential_governance_ready"),
        "frontend_secrets_blocked": not credentials.get("frontend_secrets_allowed"),
        "audit_ledger_ready": audit.get("audit_ledger_ready"),
        "governance_enforcement_ready": enforcement.get("governance_enforcement_ready"),
        "external_boundary_ready": boundary.get("external_boundary_ready"),
        "external_online_blocked": not boundary.get("online_transition_allowed"),
        "security_monitoring_ready": monitoring.get("security_monitoring_ready"),
        "approval_workflow_ready": approval.get("approval_workflow_ready"),
        "kill_switch_ready": kill_switch.get("kill_switch_ready"),
        "compliance_provenance_ready": provenance.get("compliance_provenance_ready"),
    }

    failed = [
        name for name, passed in checks.items()
        if not passed
    ]

    return {
        "artifact": "oc_deployment_readiness_validator_v1",
        "layer": "OracleChambers Deployment Readiness Validator",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "phase_f_complete": len(failed) == 0,
        "offline_institutional_security_ready": len(failed) == 0,
        "online_transition_allowed": False,
        "failed_checks": failed,
        "checks": checks,
        "validated_layers": [
            identity["artifact"],
            credentials["artifact"],
            audit["artifact"],
            enforcement["artifact"],
            boundary["artifact"],
            monitoring["artifact"],
            approval["artifact"],
            kill_switch["artifact"],
            provenance["artifact"],
        ],
    }


if __name__ == "__main__":
    print(build_deployment_readiness_validator_v1())
    
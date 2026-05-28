from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from oc_institutional_security_monitoring_v1 import (
    build_institutional_security_monitoring_v1,
)
from oc_executive_approval_workflow_v1 import (
    build_executive_approval_workflow_v1,
)
from oc_runtime_kill_switch_v1 import (
    build_runtime_kill_switch_v1,
)
from oc_compliance_provenance_layer_v1 import (
    build_compliance_provenance_layer_v1,
)
from oc_deployment_readiness_validator_v1 import (
    build_deployment_readiness_validator_v1,
)


def main() -> None:
    monitoring = build_institutional_security_monitoring_v1()
    approval = build_executive_approval_workflow_v1()
    kill_switch = build_runtime_kill_switch_v1()
    provenance = build_compliance_provenance_layer_v1()
    readiness = build_deployment_readiness_validator_v1()

    failures: list[str] = []

    if not monitoring.get("security_monitoring_ready"):
        failures.append("security_monitoring_not_ready")

    if not approval.get("approval_workflow_ready"):
        failures.append("approval_workflow_not_ready")

    if not kill_switch.get("kill_switch_ready"):
        failures.append("kill_switch_not_ready")

    if not provenance.get("compliance_provenance_ready"):
        failures.append("compliance_provenance_not_ready")

    if not readiness.get("phase_f_complete"):
        failures.append("phase_f_not_complete")

    for layer in [
        monitoring,
        approval,
        kill_switch,
        provenance,
        readiness,
    ]:
        if layer.get("online_transition_allowed"):
            failures.append(
                f"online_gate_open:{layer.get('artifact')}"
            )

    result: dict[str, Any] = {
        "artifact": "test_phase_f_security_layers_6_10_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "passed": len(failures) == 0,
        "failed_checks": failures,
        "validated_layers": [
            monitoring["artifact"],
            approval["artifact"],
            kill_switch["artifact"],
            provenance["artifact"],
            readiness["artifact"],
        ],
    }

    print(result)

    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

    
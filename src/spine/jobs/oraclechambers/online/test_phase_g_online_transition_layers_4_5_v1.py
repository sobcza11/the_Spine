from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from oc_hybrid_offline_online_sync_v1 import (
    build_hybrid_offline_online_sync_v1,
)

from oc_executive_online_deployment_gate_v1 import (
    build_executive_online_deployment_gate_v1,
    evaluate_executive_online_gate_v1,
)


def main() -> None:
    sync = build_hybrid_offline_online_sync_v1()
    gate = build_executive_online_deployment_gate_v1()

    failures: list[str] = []

    if not sync.get("hybrid_sync_ready"):
        failures.append("hybrid_sync_not_ready")

    if sync.get("online_transition_allowed"):
        failures.append("hybrid_sync_gate_open")

    if not gate.get("executive_online_gate_ready"):
        failures.append("executive_gate_not_ready")

    if gate.get("default_online_transition_allowed"):
        failures.append("executive_default_gate_should_be_blocked")

    test_approved = evaluate_executive_online_gate_v1(
        actor_role="deployment_approver",
        requested_tier="hybrid_staging",
        offline_validation_passed=True,
        security_validation_passed=True,
        mirror_validation_passed=True,
    )

    test_blocked = evaluate_executive_online_gate_v1(
        actor_role="runtime_operator",
        requested_tier="institutional_online",
        offline_validation_passed=True,
        security_validation_passed=True,
        mirror_validation_passed=True,
    )

    if not test_approved.get("online_transition_allowed"):
        failures.append("valid_gate_approval_failed")

    if test_blocked.get("online_transition_allowed"):
        failures.append("runtime_operator_should_not_approve_online")

    result: dict[str, Any] = {
        "artifact": "test_phase_g_online_transition_layers_4_5_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "passed": len(failures) == 0,
        "failed_checks": failures,
        "validated_layers": [
            sync["artifact"],
            gate["artifact"],
        ],
        "positive_gate_test": test_approved,
        "negative_gate_test": test_blocked,
    }

    print(result)

    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

    
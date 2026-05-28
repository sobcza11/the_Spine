from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4


APPROVAL_CHAIN = [
    "runtime_operator_review",
    "governance_review",
    "security_review",
    "deployment_approver_signoff",
    "executive_final_gate",
]


def evaluate_executive_online_gate_v1(
    actor_role: str,
    requested_tier: str,
    offline_validation_passed: bool,
    security_validation_passed: bool,
    mirror_validation_passed: bool,
) -> dict[str, Any]:
    approved_actor = actor_role == "deployment_approver"
    approved_tier = requested_tier in {
        "hybrid_staging",
        "institutional_online",
    }

    allowed = (
        approved_actor
        and approved_tier
        and offline_validation_passed
        and security_validation_passed
        and mirror_validation_passed
    )

    return {
        "gate_request_id": str(uuid4()),
        "artifact": "oc_executive_online_gate_decision_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "actor_role": actor_role,
        "requested_tier": requested_tier,
        "offline_validation_passed": offline_validation_passed,
        "security_validation_passed": security_validation_passed,
        "mirror_validation_passed": mirror_validation_passed,
        "online_transition_allowed": allowed,
        "approval_chain": APPROVAL_CHAIN,
        "decision": "approved" if allowed else "blocked",
        "reason": (
            "Online transition approved by executive deployment gate."
            if allowed
            else "Online transition blocked pending full validation chain."
        ),
    }


def build_executive_online_deployment_gate_v1() -> dict[str, Any]:
    blocked_test = evaluate_executive_online_gate_v1(
        actor_role="runtime_operator",
        requested_tier="institutional_online",
        offline_validation_passed=True,
        security_validation_passed=True,
        mirror_validation_passed=True,
    )

    staged_test = evaluate_executive_online_gate_v1(
        actor_role="deployment_approver",
        requested_tier="hybrid_staging",
        offline_validation_passed=True,
        security_validation_passed=True,
        mirror_validation_passed=False,
    )

    return {
        "artifact": "oc_executive_online_deployment_gate_v1",
        "layer": "OracleChambers Executive Online Deployment Gate",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "executive_online_gate_ready": True,
        "default_online_transition_allowed": False,
        "approval_chain": APPROVAL_CHAIN,
        "test_decisions": [
            blocked_test,
            staged_test,
        ],
    }


if __name__ == "__main__":
    print(build_executive_online_deployment_gate_v1())

    
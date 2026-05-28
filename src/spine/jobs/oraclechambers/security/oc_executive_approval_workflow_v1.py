from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from oc_runtime_audit_ledger_v1 import append_audit_event


APPROVAL_STEPS = [
    "runtime_operator_review",
    "governance_review",
    "security_review",
    "deployment_approver_signoff",
]


def request_deployment_approval(
    actor_role: str,
    requested_tier: str,
) -> dict[str, Any]:
    allowed = (
        actor_role == "deployment_approver"
        and requested_tier in {"hybrid_staging", "institutional_online"}
    )

    reason = (
        "Deployment approval request accepted for gated review."
        if allowed
        else "Deployment approval request denied by workflow policy."
    )

    audit_event = append_audit_event(
        event_type="deployment_approval_requested",
        actor_role=actor_role,
        action=f"request_deployment_approval:{requested_tier}",
        allowed=allowed,
        reason=reason,
    )

    return {
        "approval_id": str(uuid4()),
        "actor_role": actor_role,
        "requested_tier": requested_tier,
        "accepted_for_review": allowed,
        "final_online_transition_allowed": False,
        "required_steps": APPROVAL_STEPS,
        "reason": reason,
        "audit_event_id": audit_event["event_id"],
    }


def build_executive_approval_workflow_v1() -> dict[str, Any]:
    sample_requests = [
        request_deployment_approval(
            actor_role="runtime_operator",
            requested_tier="institutional_online",
        ),
        request_deployment_approval(
            actor_role="deployment_approver",
            requested_tier="hybrid_staging",
        ),
    ]

    return {
        "artifact": "oc_executive_approval_workflow_v1",
        "layer": "OracleChambers Executive Approval Workflow",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "approval_workflow_ready": True,
        "online_transition_allowed": False,
        "sample_requests": sample_requests,
    }


if __name__ == "__main__":
    print(build_executive_approval_workflow_v1())

    
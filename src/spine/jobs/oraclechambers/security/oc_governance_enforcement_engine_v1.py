from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from oc_identity_access_governance_v1 import ROLES
from oc_runtime_audit_ledger_v1 import append_audit_event


PROHIBITED_ACTIONS = {
    "ai_own_runtime_truth",
    "ai_mutate_governance",
    "use_unapproved_source",
    "open_online_transition_without_approval",
    "store_secret_in_frontend",
}


def evaluate_governance_action(
    actor_role: str,
    action: str,
) -> dict[str, Any]:
    role = ROLES.get(actor_role)

    if role is None:
        allowed = False
        reason = "Unknown actor role."
    elif action in PROHIBITED_ACTIONS:
        allowed = False
        reason = "Action prohibited by governance policy."
    elif action == "mutate_runtime":
        allowed = bool(role["can_mutate_runtime"])
        reason = "Runtime mutation requires runtime_operator role."
    elif action == "approve_deployment":
        allowed = bool(role["can_approve_deployment"])
        reason = "Deployment approval requires deployment_approver role."
    elif action == "view_runtime":
        allowed = bool(role["can_view"])
        reason = "View access governed by role policy."
    else:
        allowed = False
        reason = "Unknown action defaults to deny."

    audit_event = append_audit_event(
        event_type="governance_action_evaluated",
        actor_role=actor_role,
        action=action,
        allowed=allowed,
        reason=reason,
    )

    return {
        "artifact": "oc_governance_action_evaluation_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "actor_role": actor_role,
        "action": action,
        "allowed": allowed,
        "reason": reason,
        "audit_event_id": audit_event["event_id"],
    }


def build_governance_enforcement_engine_v1() -> dict[str, Any]:
    checks = [
        evaluate_governance_action("executive_viewer", "view_runtime"),
        evaluate_governance_action("executive_viewer", "mutate_runtime"),
        evaluate_governance_action("runtime_operator", "mutate_runtime"),
        evaluate_governance_action("deployment_approver", "approve_deployment"),
        evaluate_governance_action("runtime_operator", "open_online_transition_without_approval"),
        evaluate_governance_action("analyst_operator", "store_secret_in_frontend"),
    ]

    return {
        "artifact": "oc_governance_enforcement_engine_v1",
        "layer": "OracleChambers Governance Enforcement Engine",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "governance_enforcement_ready": True,
        "default_policy": "deny",
        "checks": checks,
    }


if __name__ == "__main__":
    print(build_governance_enforcement_engine_v1())
    
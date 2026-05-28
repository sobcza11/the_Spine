from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from oc_runtime_audit_ledger_v1 import append_audit_event


def trigger_kill_switch(
    actor_role: str,
    reason: str,
) -> dict[str, Any]:
    allowed_roles = {
        "runtime_operator",
        "deployment_approver",
    }

    allowed = actor_role in allowed_roles

    audit_event = append_audit_event(
        event_type="runtime_kill_switch_requested",
        actor_role=actor_role,
        action="trigger_runtime_kill_switch",
        allowed=allowed,
        reason=reason,
    )

    return {
        "artifact": "oc_runtime_kill_switch_event_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "actor_role": actor_role,
        "allowed": allowed,
        "runtime_frozen": allowed,
        "online_transition_allowed": False,
        "reason": reason,
        "audit_event_id": audit_event["event_id"],
    }


def build_runtime_kill_switch_v1() -> dict[str, Any]:
    test_events = [
        trigger_kill_switch(
            actor_role="executive_viewer",
            reason="Unauthorized viewer test.",
        ),
        trigger_kill_switch(
            actor_role="runtime_operator",
            reason="Authorized runtime isolation test.",
        ),
    ]

    return {
        "artifact": "oc_runtime_kill_switch_v1",
        "layer": "OracleChambers Runtime Kill-Switch Layer",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "kill_switch_ready": True,
        "online_transition_allowed": False,
        "test_events": test_events,
    }


if __name__ == "__main__":
    print(build_runtime_kill_switch_v1())
    
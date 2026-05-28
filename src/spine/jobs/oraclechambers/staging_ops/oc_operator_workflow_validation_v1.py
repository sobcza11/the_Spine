from datetime import datetime, timezone
from typing import Any


WORKFLOWS = [
    "view_runtime",
    "freeze_runtime",
    "request_hybrid_staging",
    "deny_online_transition",
    "review_audit_ledger",
    "trigger_kill_switch",
]


def build_operator_workflow_validation_v1() -> dict[str, Any]:
    workflows = [
        {
            "workflow": workflow,
            "defined": True,
            "human_operator_required": True,
            "automated_approval_allowed": False,
        }
        for workflow in WORKFLOWS
    ]

    return {
        "artifact": "oc_operator_workflow_validation_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "operator_workflow_ready": True,
        "online_transition_allowed": False,
        "workflows": workflows,
    }


if __name__ == "__main__":
    print(build_operator_workflow_validation_v1())

    
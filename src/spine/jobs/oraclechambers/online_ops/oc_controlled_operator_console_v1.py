from datetime import datetime, timezone
from typing import Any


def build_controlled_operator_console_v1() -> dict[str, Any]:
    return {
        "artifact": "oc_controlled_operator_console_v1",
        "layer": "Controlled Operator Console",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "operator_console_ready": True,
        "online_transition_allowed": False,
        "operator_actions": {
            "view_runtime": True,
            "freeze_runtime": True,
            "unfreeze_runtime": False,
            "approve_online_transition": False,
            "override_governance": False,
        },
        "required_roles": {
            "freeze_runtime": ["runtime_operator", "deployment_approver"],
            "unfreeze_runtime": ["deployment_approver"],
            "approve_online_transition": ["deployment_approver"],
        },
    }


if __name__ == "__main__":
    print(build_controlled_operator_console_v1())

    
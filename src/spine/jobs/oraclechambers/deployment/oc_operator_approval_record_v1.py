from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import json
from uuid import uuid4


REPO_ROOT = Path(__file__).resolve().parents[5]
OUT = REPO_ROOT / "data" / "deployment" / "oraclechambers" / "oc_operator_approval_record_v1.json"


def build_operator_approval_record_v1() -> dict[str, Any]:
    record = {
        "artifact": "oc_operator_approval_record_v1",
        "approval_id": str(uuid4()),
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "approval_scope": "HYBRID_STAGING_REVIEW",
        "operator_role_required": "deployment_approver",
        "approval_granted": False,
        "online_transition_allowed": False,
        "reason": "Approval record exists, but authorization is not granted by default.",
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(record, indent=2), encoding="utf-8")
    return record


if __name__ == "__main__":
    print(build_operator_approval_record_v1())

    
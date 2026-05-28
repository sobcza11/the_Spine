from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import json


REPO_ROOT = Path(__file__).resolve().parents[5]
OUT = REPO_ROOT / "data" / "deployment" / "oraclechambers" / "oc_containerization_contract_v1.json"


def build_containerization_contract_v1() -> dict[str, Any]:
    contract = {
        "artifact": "oc_containerization_contract_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "containerization_ready": True,
        "dockerfile_required": True,
        "compose_required": True,
        "secrets_mounted_at_runtime": True,
        "frontend_secrets_allowed": False,
        "online_transition_allowed": False,
        "images": [
            "oraclechambers-api",
            "oraclechambers-offline-site",
            "oraclechambers-staging-gateway",
        ],
        "container_build_executed": False,
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(contract, indent=2), encoding="utf-8")

    return contract


if __name__ == "__main__":
    print(build_containerization_contract_v1())

    
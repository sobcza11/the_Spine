from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import json


REPO_ROOT = Path(__file__).resolve().parents[5]
OUT = REPO_ROOT / "data" / "deployment" / "oraclechambers" / "oc_ci_cd_release_pipeline_contract_v1.json"


def build_ci_cd_release_pipeline_contract_v1() -> dict[str, Any]:
    pipeline = {
        "artifact": "oc_ci_cd_release_pipeline_contract_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "ci_cd_contract_ready": True,
        "online_transition_allowed": False,
        "required_stages": [
            "lint",
            "unit_tests",
            "offline_rc_validation",
            "security_validation",
            "runtime_standardization",
            "container_build",
            "staging_preflight",
            "manual_approval",
        ],
        "manual_approval_required": True,
        "automatic_online_deploy_allowed": False,
        "pipeline_executed": False,
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(pipeline, indent=2), encoding="utf-8")

    return pipeline


if __name__ == "__main__":
    print(build_ci_cd_release_pipeline_contract_v1())

    
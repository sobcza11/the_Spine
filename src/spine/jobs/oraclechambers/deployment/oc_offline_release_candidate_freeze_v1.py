import json
from datetime import datetime, timezone
from pathlib import Path
import sys

CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

from oc_repo_runtime_bootstrap_v1 import bootstrap_repo_root
from oc_repo_path_governance_v1 import repo_path

REPO_ROOT = bootstrap_repo_root()

FREEZE_PATH = repo_path(
    "data",
    "deployment",
    "oraclechambers",
    "oc_offline_release_candidate_freeze_v1.json"
)


def main():

    freeze_state = {
        "artifact": "oc_offline_release_candidate_freeze_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "repo_root": str(REPO_ROOT),
        "release_candidate": "offline_rc_v1",
        "runtime_mutation_locked": True,
        "online_transition_allowed": False,
        "ai_runtime_mutation_allowed": False,
        "deployment_state": "FROZEN_FOR_VALIDATION",
        "approved_next_step": "institutional_offline_validation"
    }

    FREEZE_PATH.parent.mkdir(parents=True, exist_ok=True)

    FREEZE_PATH.write_text(
        json.dumps(freeze_state, indent=2),
        encoding="utf-8"
    )

    print(freeze_state)


if __name__ == "__main__":
    main()


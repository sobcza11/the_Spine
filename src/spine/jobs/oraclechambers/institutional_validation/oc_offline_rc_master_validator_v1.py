from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys

DEPLOYMENT_DIR = Path(__file__).resolve().parents[1] / "deployment"

if str(DEPLOYMENT_DIR) not in sys.path:
    sys.path.insert(0, str(DEPLOYMENT_DIR))

from oc_offline_site_inventory_v1 import build_offline_site_inventory_v1


REPO_ROOT = Path(__file__).resolve().parents[5]

STANDARDIZATION_TEST = (
    REPO_ROOT
    / "src"
    / "spine"
    / "jobs"
    / "oraclechambers"
    / "deployment"
    / "oc_runtime_standardization_validator_v1.py"
)


def run_standardization_validator() -> bool:
    result = subprocess.run(
        [sys.executable, str(STANDARDIZATION_TEST)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )

    return result.returncode == 0 and "standardization_passed': True" in result.stdout


def build_offline_rc_master_validator_v1():
    inventory = build_offline_site_inventory_v1()
    standardization_passed = run_standardization_validator()

    passed = (
        inventory["inventory_ready"]
        and standardization_passed
    )

    output = {
        "artifact": "oc_offline_rc_master_validator_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "offline_rc_validated": passed,
        "online_transition_allowed": False,
        "checks": {
            "inventory_ready": inventory["inventory_ready"],
            "runtime_standardization_passed": standardization_passed,
        },
    }

    print(output)


if __name__ == "__main__":
    build_offline_rc_master_validator_v1()
    


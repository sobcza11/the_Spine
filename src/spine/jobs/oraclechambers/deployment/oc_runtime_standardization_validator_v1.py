from pathlib import Path
import subprocess
import sys
from datetime import datetime, timezone

CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

from oc_repo_runtime_bootstrap_v1 import bootstrap_repo_root

REPO_ROOT = bootstrap_repo_root()

TESTS = [
    "src/spine/jobs/oraclechambers/runtime/test_phase_e_runtime_layers_1_3_v1.py",
    "src/spine/jobs/oraclechambers/runtime/test_phase_e_runtime_layers_4_6_v1.py",
    "src/spine/jobs/oraclechambers/security/test_phase_f_security_layers_1_5_v1.py",
    "src/spine/jobs/oraclechambers/security/test_phase_f_security_layers_6_10_v1.py",
    "src/spine/jobs/oraclechambers/online/test_phase_g_online_transition_layers_1_3_v1.py",
    "src/spine/jobs/oraclechambers/online/test_phase_g_online_transition_layers_4_5_v1.py"
]


def run_test(path_str):

    full_path = REPO_ROOT / path_str

    try:

        result = subprocess.run(
            [sys.executable, str(full_path)],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True
        )

        passed = result.returncode == 0

        return {
            "test": path_str,
            "passed": passed,
            "stderr": result.stderr[-500:] if result.stderr else None
        }

    except Exception as e:

        return {
            "test": path_str,
            "passed": False,
            "stderr": str(e)
        }


def main():

    results = [run_test(t) for t in TESTS]

    failed = [r for r in results if not r["passed"]]

    output = {
        "artifact": "oc_runtime_standardization_validator_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "repo_root": str(REPO_ROOT),
        "standardization_passed": len(failed) == 0,
        "failed_tests": len(failed),
        "results": results
    }

    print(output)


if __name__ == "__main__":
    main()


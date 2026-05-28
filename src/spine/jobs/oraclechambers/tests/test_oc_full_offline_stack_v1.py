from __future__ import annotations

import subprocess
import sys


COMMANDS = [
    "python src/spine/jobs/oraclechambers/entrypoint/run_oc_local_deployment_v1.py",
    "python src/spine/jobs/oraclechambers/tests/test_oc_local_deployment_entrypoint_v1.py",
]


def main() -> None:
    failed = []

    print("OC Full Offline Stack Test v1")
    print("-" * 60)

    for command in COMMANDS:
        print(f"\nRUNNING: {command}")

        result = subprocess.run(
            command,
            shell=True,
            text=True,
        )

        if result.returncode != 0:
            failed.append(command)
            break

    print("\n" + "-" * 60)

    if failed:
        print("full_stack_passed: False")
        print(f"failed_command: {failed[0]}")
        sys.exit(1)

    print("full_stack_passed: True")
    print("deployment_mode: offline_first")
    print("status: OracleChambers + GeoScen local institutional stack validated")


if __name__ == "__main__":
    main()
    
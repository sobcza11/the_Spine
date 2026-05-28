from __future__ import annotations

import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


RUN_TS = datetime.now(timezone.utc).isoformat()

COMMANDS = [
    "python src/spine/jobs/geoscen/serving/build_geoscen_unified_serving_v1.py",
    "python src/spine/jobs/geoscen/regime/build_geoscen_unified_regime_engine_v1.py",
    "python src/spine/jobs/geoscen/metrics/build_geoscen_final_metric_engine_v1.py",
    "python src/spine/jobs/geoscen/history/build_geoscen_historical_regime_memory_v1.py",
    "python src/spine/jobs/geoscen/synthesis/build_geoscen_institutional_synthesis_v1.py",
    "python src/spine/jobs/geoscen/tests/test_geoscen_deployment_chain_v1.py",
    "python src/spine/jobs/oraclechambers/ingestion/build_oc_geoscen_ingestion_bridge_v1.py",
    "python src/spine/jobs/oraclechambers/frontend/build_oc_local_intelligence_layer_v1.py",
    "python src/spine/jobs/oraclechambers/runtime/build_oc_runtime_controller_v1.py",
    "python src/spine/jobs/oraclechambers/tests/test_oc_runtime_stack_v1.py",
    "python src/spine/jobs/oraclechambers/frontend/build_oc_adaptive_layout_engine_v1.py",
    "python src/spine/jobs/oraclechambers/frontend/build_oc_executive_dashboard_state_v1.py",
    "python src/spine/jobs/oraclechambers/tests/test_oc_executive_dashboard_stack_v1.py",
    "python src/spine/jobs/oraclechambers/manifest/build_oc_local_deployment_manifest_v1.py",
    "python src/spine/jobs/oraclechambers/tests/test_oc_local_deployment_manifest_v1.py",
    "python src/spine/jobs/oraclechambers/site/build_oc_local_site_hydration_v1.py",
    "python src/spine/jobs/oraclechambers/tests/test_oc_local_site_hydration_v1.py",
    "python src/spine/jobs/oraclechambers/runtime/build_oc_local_launch_controller_v1.py",
]


def run_command(command: str) -> bool:
    print("\n" + "=" * 80)
    print(f"RUNNING: {command}")
    print("=" * 80)

    result = subprocess.run(
        command,
        shell=True,
        text=True,
    )

    return result.returncode == 0


def main() -> None:
    print("OracleChambers Local Deployment v1")
    print(f"run_ts: {RUN_TS}")
    print("deployment_mode: offline_first")
    print("ai_dependency: False")

    failed_commands = []

    for command in COMMANDS:
        passed = run_command(command)

        if not passed:
            failed_commands.append(command)
            break

    print("\n" + "=" * 80)
    print("OC Local Deployment Run Complete")
    print("=" * 80)

    if failed_commands:
        print("deployment_ready: False")
        print("failed_command:")
        print(failed_commands[0])
        sys.exit(1)

    print("deployment_ready: True")
    print("status: OracleChambers offline institutional stack complete")


if __name__ == "__main__":
    main()

    
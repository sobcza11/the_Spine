from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


INPUT_JSON = Path(
    "data/serving/oraclechambers/oc_local_launch_controller_v1.json"
)

INPUT_PARQUET = Path(
    "data/serving/oraclechambers/oc_local_launch_controller_v1.parquet"
)

OUTPUT_PARQUET = Path(
    "data/serving/oraclechambers/oc_local_deployment_entrypoint_test_v1.parquet"
)


REQUIRED_KEYS = [
    "artifact",
    "system",
    "layer",
    "version",
    "deployment_ready",
    "boot_sequence",
    "runtime_launch_state",
    "dashboard_launch_state",
    "routing",
    "provenance",
]


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(
            f"Missing required input: {path}"
        )

    with path.open(
        "r",
        encoding="utf-8",
    ) as f:
        return json.load(f)


def validate_required_keys(payload: Dict[str, Any]) -> List[str]:
    failures: List[str] = []

    for key in REQUIRED_KEYS:
        if key not in payload:
            failures.append(f"missing_key::{key}")

    return failures


def validate_boot_sequence(payload: Dict[str, Any]) -> List[str]:
    failures: List[str] = []

    boot_sequence = payload.get("boot_sequence", [])

    if not boot_sequence:
        failures.append("empty_boot_sequence")
        return failures

    for step in boot_sequence:
        if step.get("deployment_ready") is not True:
            failures.append(
                f"boot_step_not_ready::{step.get('stage')}"
            )

        if step.get("status") != "ready":
            failures.append(
                f"boot_step_status_not_ready::{step.get('stage')}"
            )

    return failures


def validate_runtime_launch_state(payload: Dict[str, Any]) -> List[str]:
    failures: List[str] = []

    runtime = payload.get("runtime_launch_state", {})

    if runtime.get("offline_first") is not True:
        failures.append("offline_first_must_be_true")

    if runtime.get("online_enabled") is not False:
        failures.append("online_enabled_must_be_false")

    if runtime.get("adaptive_routing_enabled") is not True:
        failures.append("adaptive_routing_not_enabled")

    return failures


def validate_dashboard_launch_state(payload: Dict[str, Any]) -> List[str]:
    failures: List[str] = []

    dashboard = payload.get("dashboard_launch_state", {})

    required_dashboard_keys = [
        "site_title",
        "site_mode",
        "runtime_mode",
        "headline_regime",
        "headline_confidence",
        "macro_temperature",
        "risk_posture",
    ]

    for key in required_dashboard_keys:
        if key not in dashboard:
            failures.append(
                f"missing_dashboard_launch_key::{key}"
            )

    if dashboard.get("site_mode") != "offline_first":
        failures.append("site_mode_must_be_offline_first")

    return failures


def validate_routing(payload: Dict[str, Any]) -> List[str]:
    failures: List[str] = []

    routing = payload.get("routing", {})

    if routing.get("local_launch_ready") is not True:
        failures.append("local_launch_ready_must_be_true")

    if routing.get("offline_first") is not True:
        failures.append("offline_first_must_be_true")

    if routing.get("online_runtime_ready") is not False:
        failures.append("online_runtime_ready_must_be_false")

    if routing.get("ai_dependency") is not False:
        failures.append("ai_dependency_must_be_false")

    return failures


def validate_provenance(payload: Dict[str, Any]) -> List[str]:
    failures: List[str] = []

    provenance = payload.get("provenance", {})

    required_keys = [
        "source_payload",
        "source_artifact",
        "source_run_ts",
    ]

    for key in required_keys:
        if key not in provenance:
            failures.append(
                f"missing_provenance_key::{key}"
            )

    return failures


def validate_parquet() -> List[str]:
    failures: List[str] = []

    if not INPUT_PARQUET.exists():
        failures.append("missing_launch_controller_parquet")
        return failures

    try:
        df = pd.read_parquet(INPUT_PARQUET)

        if df.empty:
            failures.append("empty_launch_controller_parquet")

    except Exception as e:
        failures.append(f"parquet_read_error::{e}")

    return failures


def run_validation() -> pd.DataFrame:
    failures: List[str] = []

    payload = load_json(INPUT_JSON)

    failures.extend(validate_required_keys(payload))
    failures.extend(validate_boot_sequence(payload))
    failures.extend(validate_runtime_launch_state(payload))
    failures.extend(validate_dashboard_launch_state(payload))
    failures.extend(validate_routing(payload))
    failures.extend(validate_provenance(payload))
    failures.extend(validate_parquet())

    return pd.DataFrame(
        [
            {
                "test": "oc_local_deployment_entrypoint_v1",
                "passed": len(failures) == 0,
                "failure_count": len(failures),
                "failures": failures,
            }
        ]
    )


def main() -> None:
    validation_df = run_validation()

    OUTPUT_PARQUET.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    validation_df.to_parquet(
        OUTPUT_PARQUET,
        index=False,
    )

    row = validation_df.iloc[0]

    print("OC Local Deployment Entrypoint Test v1")
    print("-" * 60)
    print(f"passed: {row['passed']}")
    print(f"failure_count: {row['failure_count']}")

    if row["failures"]:
        print("failures:")
        for failure in row["failures"]:
            print(f"  - {failure}")

    print(f"validation_output: {OUTPUT_PARQUET}")


if __name__ == "__main__":
    main()
    
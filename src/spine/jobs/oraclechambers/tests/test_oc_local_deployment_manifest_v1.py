from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


INPUT_JSON = Path(
    "data/serving/oraclechambers/oc_local_deployment_manifest_v1.json"
)

INPUT_PARQUET = Path(
    "data/serving/oraclechambers/oc_local_deployment_manifest_v1.parquet"
)

OUTPUT_PARQUET = Path(
    "data/serving/oraclechambers/oc_local_deployment_manifest_test_v1.parquet"
)


REQUIRED_KEYS = [
    "artifact",
    "system",
    "layer",
    "version",
    "deployment_ready",
    "asset_registry",
    "runtime_manifest",
    "validation_summary",
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


def validate_required_keys(
    payload: Dict[str, Any],
) -> List[str]:

    failures: List[str] = []

    for key in REQUIRED_KEYS:
        if key not in payload:
            failures.append(
                f"missing_key::{key}"
            )

    return failures


def validate_asset_registry(
    payload: Dict[str, Any],
) -> List[str]:

    failures: List[str] = []

    registry = payload.get(
        "asset_registry",
        {},
    )

    if not registry:
        failures.append(
            "empty_asset_registry"
        )
        return failures

    for asset_name, asset in registry.items():

        if not asset.get("path"):
            failures.append(
                f"{asset_name}::missing_path"
            )

        if not Path(asset["path"]).exists():
            failures.append(
                f"{asset_name}::missing_asset_file"
            )

        if asset.get("deployment_ready") is not True:
            failures.append(
                f"{asset_name}::not_deployment_ready"
            )

    return failures


def validate_runtime_manifest(
    payload: Dict[str, Any],
) -> List[str]:

    failures: List[str] = []

    runtime = payload.get(
        "runtime_manifest",
        {},
    )

    if runtime.get("deployment_mode") != "offline_first":
        failures.append(
            "deployment_mode_must_be_offline_first"
        )

    if runtime.get("local_runtime_enabled") is not True:
        failures.append(
            "local_runtime_not_enabled"
        )

    if runtime.get("online_runtime_enabled") is not False:
        failures.append(
            "online_runtime_should_be_false"
        )

    if runtime.get("ai_dependency") is not False:
        failures.append(
            "ai_dependency_must_be_false"
        )

    return failures


def validate_validation_summary(
    payload: Dict[str, Any],
) -> List[str]:

    failures: List[str] = []

    summary = payload.get(
        "validation_summary",
        {},
    )

    if summary.get("all_assets_ready") is not True:
        failures.append(
            "not_all_assets_ready"
        )

    if float(summary.get("ready_ratio", 0.0)) < 1.0:
        failures.append(
            "ready_ratio_below_1"
        )

    return failures


def validate_parquet() -> List[str]:

    failures: List[str] = []

    if not INPUT_PARQUET.exists():
        failures.append(
            "missing_manifest_parquet"
        )
        return failures

    try:
        df = pd.read_parquet(
            INPUT_PARQUET
        )

        if df.empty:
            failures.append(
                "empty_manifest_parquet"
            )

    except Exception as e:
        failures.append(
            f"parquet_read_error::{e}"
        )

    return failures


def run_validation() -> pd.DataFrame:

    failures: List[str] = []

    payload = load_json(
        INPUT_JSON
    )

    failures.extend(
        validate_required_keys(payload)
    )

    failures.extend(
        validate_asset_registry(payload)
    )

    failures.extend(
        validate_runtime_manifest(payload)
    )

    failures.extend(
        validate_validation_summary(payload)
    )

    failures.extend(
        validate_parquet()
    )

    return pd.DataFrame(
        [
            {
                "test": "oc_local_deployment_manifest_v1",
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

    print(
        "OC Local Deployment Manifest Test v1"
    )

    print("-" * 55)

    print(
        f"passed: {row['passed']}"
    )

    print(
        f"failure_count: {row['failure_count']}"
    )

    if row["failures"]:
        print("failures:")
        for failure in row["failures"]:
            print(f"  - {failure}")

    print(
        f"validation_output: {OUTPUT_PARQUET}"
    )


if __name__ == "__main__":
    main()

    
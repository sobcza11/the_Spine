from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


PIPELINE = [
    {
        "name": "OC GeoScen Ingestion Bridge",
        "json": Path(
            "data/serving/oraclechambers/oc_geoscen_ingestion_bridge_v1.json"
        ),
        "parquet": Path(
            "data/serving/oraclechambers/oc_geoscen_ingestion_bridge_v1.parquet"
        ),
        "required_keys": [
            "artifact",
            "panel_state",
            "visual_routing",
            "routing",
            "provenance",
        ],
    },
    {
        "name": "OC Local Intelligence Layer",
        "json": Path(
            "data/serving/oraclechambers/oc_local_intelligence_layer_v1.json"
        ),
        "parquet": Path(
            "data/serving/oraclechambers/oc_local_intelligence_layer_v1.parquet"
        ),
        "required_keys": [
            "artifact",
            "executive_state",
            "active_panels",
            "overlay_state",
            "chart_state",
            "runtime_state",
            "routing",
            "provenance",
        ],
    },
    {
        "name": "OC Runtime Controller",
        "json": Path(
            "data/serving/oraclechambers/oc_runtime_controller_v1.json"
        ),
        "parquet": Path(
            "data/serving/oraclechambers/oc_runtime_controller_v1.parquet"
        ),
        "required_keys": [
            "artifact",
            "runtime_controls",
            "panel_runtime_state",
            "overlay_runtime_state",
            "chart_runtime_state",
            "routing",
            "provenance",
        ],
    },
]


def load_json(
    path: Path
) -> Dict[str, Any]:

    with path.open(
        "r",
        encoding="utf-8",
    ) as f:
        return json.load(f)


def validate_json_exists(
    path: Path
) -> bool:

    return path.exists()


def validate_parquet_exists(
    path: Path
) -> bool:

    return path.exists()


def validate_required_keys(
    payload: Dict[str, Any],
    required_keys: List[str],
) -> List[str]:

    failures: List[str] = []

    for key in required_keys:

        if key not in payload:
            failures.append(
                f"missing_key::{key}"
            )

    return failures


def validate_parquet_read(
    path: Path
) -> List[str]:

    failures: List[str] = []

    try:

        df = pd.read_parquet(path)

        if df.empty:
            failures.append(
                "empty_parquet"
            )

    except Exception as e:

        failures.append(
            f"parquet_read_error::{e}"
        )

    return failures


def validate_provenance(
    payload: Dict[str, Any]
) -> List[str]:

    failures: List[str] = []

    provenance = payload.get(
        "provenance"
    )

    if provenance is None:
        failures.append(
            "missing_provenance"
        )

        return failures

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


def validate_routing(
    payload: Dict[str, Any]
) -> List[str]:

    failures: List[str] = []

    routing = payload.get(
        "routing"
    )

    if routing is None:
        failures.append(
            "missing_routing"
        )

        return failures

    if routing.get(
        "ai_dependency"
    ) is not False:

        failures.append(
            "ai_dependency_must_be_false"
        )

    return failures


def validate_deployment_state(
    payload: Dict[str, Any]
) -> List[str]:

    failures: List[str] = []

    if "deployment_ready" not in payload:
        failures.append(
            "missing_deployment_ready"
        )

    return failures


def run_validation() -> pd.DataFrame:

    rows: List[Dict[str, Any]] = []

    for stage in PIPELINE:

        stage_name = stage["name"]

        json_path = stage["json"]
        parquet_path = stage["parquet"]

        failures: List[str] = []

        json_exists = validate_json_exists(
            json_path
        )

        parquet_exists = validate_parquet_exists(
            parquet_path
        )

        payload: Dict[str, Any] = {}

        if not json_exists:
            failures.append(
                "missing_json"
            )

        if not parquet_exists:
            failures.append(
                "missing_parquet"
            )

        if json_exists:

            try:

                payload = load_json(
                    json_path
                )

            except Exception as e:

                failures.append(
                    f"json_load_error::{e}"
                )

        if payload:

            failures.extend(
                validate_required_keys(
                    payload,
                    stage[
                        "required_keys"
                    ],
                )
            )

            failures.extend(
                validate_provenance(
                    payload
                )
            )

            failures.extend(
                validate_routing(
                    payload
                )
            )

            failures.extend(
                validate_deployment_state(
                    payload
                )
            )

        if parquet_exists:

            failures.extend(
                validate_parquet_read(
                    parquet_path
                )
            )

        rows.append(
            {
                "stage": stage_name,
                "json_exists": json_exists,
                "parquet_exists": parquet_exists,
                "passed": len(failures)
                == 0,
                "failure_count": len(
                    failures
                ),
                "failures": failures,
            }
        )

    return pd.DataFrame(rows)


def print_summary(
    validation_df: pd.DataFrame
) -> None:

    print(
        "\nOracleChambers Runtime Stack Test v1"
    )

    print("-" * 55)

    total_stages = len(
        validation_df
    )

    passed_stages = int(
        validation_df["passed"].sum()
    )

    print(
        f"passed_stages: {passed_stages}/{total_stages}"
    )

    for _, row in validation_df.iterrows():

        print(
            "\n----------------------------------"
        )

        print(
            f"stage: {row['stage']}"
        )

        print(
            f"passed: {row['passed']}"
        )

        print(
            f"failure_count: {row['failure_count']}"
        )

        if row["failures"]:

            print("failures:")

            for failure in row[
                "failures"
            ]:

                print(
                    f"  - {failure}"
                )

    print(
        "\nRuntime Stack Validation Complete"
    )


def main() -> None:

    validation_df = run_validation()

    print_summary(
        validation_df
    )

    output_path = Path(
        "data/serving/oraclechambers/oc_runtime_stack_test_v1.parquet"
    )

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    validation_df.to_parquet(
        output_path,
        index=False,
    )

    print(
        f"\nvalidation_output: {output_path}"
    )


if __name__ == "__main__":
    main()

    
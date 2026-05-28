from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


PIPELINE = [
    {
        "name": "Unified Serving Layer",
        "json": Path(
            "data/serving/geoscen/geoscen_unified_serving_v1.json"
        ),
        "parquet": Path(
            "data/serving/geoscen/geoscen_unified_serving_v1.parquet"
        ),
        "required_keys": [
            "artifact",
            "system",
            "layer",
            "version",
            "deployment_ready",
            "routing",
            "provenance",
        ],
    },
    {
        "name": "Unified Regime Engine",
        "json": Path(
            "data/serving/geoscen/geoscen_unified_regime_engine_v1.json"
        ),
        "parquet": Path(
            "data/serving/geoscen/geoscen_unified_regime_engine_v1.parquet"
        ),
        "required_keys": [
            "artifact",
            "regime",
            "signal_table",
            "routing",
            "provenance",
        ],
    },
    {
        "name": "Final Metric Engine",
        "json": Path(
            "data/serving/geoscen/geoscen_final_metric_engine_v1.json"
        ),
        "parquet": Path(
            "data/serving/geoscen/geoscen_final_metric_engine_v1.parquet"
        ),
        "required_keys": [
            "artifact",
            "metrics",
            "routing",
            "provenance",
        ],
    },
    {
        "name": "Historical Regime Memory",
        "json": Path(
            "data/serving/geoscen/geoscen_historical_regime_memory_v1.json"
        ),
        "parquet": Path(
            "data/serving/geoscen/geoscen_historical_regime_memory_v1.parquet"
        ),
        "required_keys": [
            "artifact",
            "historical_matches",
            "top_match",
            "routing",
            "provenance",
        ],
    },
    {
        "name": "Institutional Synthesis Layer",
        "json": Path(
            "data/serving/geoscen/geoscen_institutional_synthesis_v1.json"
        ),
        "parquet": Path(
            "data/serving/geoscen/geoscen_institutional_synthesis_v1.parquet"
        ),
        "required_keys": [
            "artifact",
            "synthesis",
            "routing",
            "provenance",
        ],
    },
]


def load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def validate_json_exists(path: Path) -> bool:
    return path.exists()


def validate_parquet_exists(path: Path) -> bool:
    return path.exists()


def validate_json_structure(
    payload: Dict[str, Any],
    required_keys: List[str],
) -> List[str]:

    failures: List[str] = []

    for key in required_keys:
        if key not in payload:
            failures.append(f"missing_key::{key}")

    return failures


def validate_parquet_read(path: Path) -> List[str]:
    failures: List[str] = []

    try:
        df = pd.read_parquet(path)

        if df.empty:
            failures.append("empty_parquet")

    except Exception as e:
        failures.append(f"parquet_read_error::{e}")

    return failures


def validate_provenance(
    payload: Dict[str, Any]
) -> List[str]:

    failures: List[str] = []

    provenance = payload.get("provenance")

    if provenance is None:
        failures.append("missing_provenance")
        return failures

    if not isinstance(provenance, dict):
        failures.append("invalid_provenance_type")
        return failures

    expected_keys = [
        "source_payload",
        "source_artifact",
        "source_run_ts",
    ]

    for key in expected_keys:
        if key not in provenance:
            failures.append(
                f"missing_provenance_key::{key}"
            )

    return failures


def validate_routing(
    payload: Dict[str, Any]
) -> List[str]:

    failures: List[str] = []

    routing = payload.get("routing")

    if routing is None:
        failures.append("missing_routing")
        return failures

    if not isinstance(routing, dict):
        failures.append("invalid_routing_type")
        return failures

    if not routing.get("ai_dependency") is False:
        failures.append(
            "ai_dependency_must_be_false"
        )

    return failures


def validate_deployment_flag(
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
            failures.append("missing_json")

        if not parquet_exists:
            failures.append("missing_parquet")

        if json_exists:

            try:
                payload = load_json(json_path)

            except Exception as e:
                failures.append(
                    f"json_load_error::{e}"
                )

        if payload:

            failures.extend(
                validate_json_structure(
                    payload,
                    stage["required_keys"],
                )
            )

            failures.extend(
                validate_provenance(payload)
            )

            failures.extend(
                validate_routing(payload)
            )

            failures.extend(
                validate_deployment_flag(payload)
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
                "passed": len(failures) == 0,
                "failure_count": len(failures),
                "failures": failures,
            }
        )

    return pd.DataFrame(rows)


def print_summary(
    validation_df: pd.DataFrame
) -> None:

    print("\nGeoScen Deployment Chain Test v1")
    print("-" * 50)

    total_stages = len(validation_df)

    passed_stages = int(
        validation_df["passed"].sum()
    )

    print(
        f"passed_stages: {passed_stages}/{total_stages}"
    )

    for _, row in validation_df.iterrows():

        print("\n----------------------------------")
        print(f"stage: {row['stage']}")
        print(f"passed: {row['passed']}")
        print(
            f"failure_count: {row['failure_count']}"
        )

        if row["failures"]:
            print("failures:")

            for failure in row["failures"]:
                print(f"  - {failure}")

    print("\nDeployment Chain Validation Complete")


def main() -> None:

    validation_df = run_validation()

    print_summary(validation_df)

    output_path = Path(
        "data/serving/geoscen/geoscen_deployment_chain_test_v1.parquet"
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

    
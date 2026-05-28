from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


INPUT_JSON = Path(
    "data/serving/oraclechambers/oc_local_site_hydration_v1.json"
)

INPUT_PARQUET = Path(
    "data/serving/oraclechambers/oc_local_site_hydration_v1.parquet"
)

OUTPUT_PARQUET = Path(
    "data/serving/oraclechambers/oc_local_site_hydration_test_v1.parquet"
)


REQUIRED_KEYS = [
    "artifact",
    "system",
    "layer",
    "version",
    "deployment_ready",
    "site_payload",
    "routing",
    "provenance",
]


def load_json(
    path: Path
) -> Dict[str, Any]:

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
    payload: Dict[str, Any]
) -> List[str]:

    failures: List[str] = []

    for key in REQUIRED_KEYS:

        if key not in payload:

            failures.append(
                f"missing_key::{key}"
            )

    return failures


def validate_site_payload(
    payload: Dict[str, Any]
) -> List[str]:

    failures: List[str] = []

    site_payload = payload.get(
        "site_payload",
        {},
    )

    required_site_keys = [
        "site_title",
        "site_mode",
        "runtime_mode",
        "deployment_ready",
        "headline",
        "dashboard",
        "frontend",
        "narrative",
        "historical_memory",
    ]

    for key in required_site_keys:

        if key not in site_payload:

            failures.append(
                f"missing_site_key::{key}"
            )

    return failures


def validate_headline(
    payload: Dict[str, Any]
) -> List[str]:

    failures: List[str] = []

    headline = payload[
        "site_payload"
    ].get(
        "headline",
        {},
    )

    required_headline_keys = [
        "regime",
        "confidence",
        "conviction",
        "macro_temperature",
        "risk_posture",
        "decision_bias",
    ]

    for key in required_headline_keys:

        if key not in headline:

            failures.append(
                f"missing_headline_key::{key}"
            )

    return failures


def validate_runtime_state(
    payload: Dict[str, Any]
) -> List[str]:

    failures: List[str] = []

    routing = payload.get(
        "routing",
        {},
    )

    if routing.get(
        "offline_first"
    ) is not True:

        failures.append(
            "offline_first_must_be_true"
        )

    if routing.get(
        "online_runtime_ready"
    ) is not False:

        failures.append(
            "online_runtime_should_be_false"
        )

    if routing.get(
        "ai_dependency"
    ) is not False:

        failures.append(
            "ai_dependency_must_be_false"
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


def validate_parquet() -> List[str]:

    failures: List[str] = []

    if not INPUT_PARQUET.exists():

        failures.append(
            "missing_site_hydration_parquet"
        )

        return failures

    try:

        df = pd.read_parquet(
            INPUT_PARQUET
        )

        if df.empty:

            failures.append(
                "empty_site_hydration_parquet"
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
        validate_required_keys(
            payload
        )
    )

    failures.extend(
        validate_site_payload(
            payload
        )
    )

    failures.extend(
        validate_headline(
            payload
        )
    )

    failures.extend(
        validate_runtime_state(
            payload
        )
    )

    failures.extend(
        validate_provenance(
            payload
        )
    )

    failures.extend(
        validate_parquet()
    )

    return pd.DataFrame(
        [
            {
                "test": "oc_local_site_hydration_v1",
                "passed": len(failures)
                == 0,
                "failure_count": len(
                    failures
                ),
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
        "OC Local Site Hydration Test v1"
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

        for failure in row[
            "failures"
        ]:

            print(
                f"  - {failure}"
            )

    print(
        f"validation_output: {OUTPUT_PARQUET}"
    )


if __name__ == "__main__":
    main()
    
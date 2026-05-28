from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


INPUT_JSON = Path(
    "data/serving/oraclechambers/oc_receiving_area_registry_v1.json"
)

INPUT_PARQUET = Path(
    "data/serving/oraclechambers/oc_receiving_area_registry_v1.parquet"
)

OUTPUT_PARQUET = Path(
    "data/serving/oraclechambers/oc_receiving_area_registry_validation_v1.parquet"
)


EXPECTED_DOMAIN_ORDER = [
    "FX",
    "RATES",
    "C_FLOW",
    "EQUITIES_INDEX",
    "EQUITIES_SECTOR",
    "CROSS_DOMAIN_OVERLAY",
    "EMBEDDED_WITHIN_ALL_DOMAINS",
]

EXPECTED_COMPARTMENT_ORDER = [
    "RBL",
    "ZT",
    "METRIC",
    "NLP",
]


def load_json() -> Dict[str, Any]:

    if not INPUT_JSON.exists():
        raise FileNotFoundError(
            f"Missing input file: {INPUT_JSON}"
        )

    with INPUT_JSON.open(
        "r",
        encoding="utf-8",
    ) as f:

        return json.load(f)


def validate_required_keys(
    payload: Dict[str, Any]
) -> List[str]:

    failures: List[str] = []

    required_keys = [
        "artifact",
        "system",
        "layer",
        "version",
        "domain_build_order",
        "compartment_order",
        "domain_registry",
        "cross_domain_overlay_layers",
        "embedded_layers",
        "routing",
        "provenance",
    ]

    for key in required_keys:

        if key not in payload:

            failures.append(
                f"missing_key::{key}"
            )

    return failures


def validate_domain_order(
    payload: Dict[str, Any]
) -> List[str]:

    failures: List[str] = []

    actual = payload.get(
        "domain_build_order",
        []
    )

    if actual != EXPECTED_DOMAIN_ORDER:

        failures.append(
            "invalid_domain_build_order"
        )

    return failures


def validate_compartment_order(
    payload: Dict[str, Any]
) -> List[str]:

    failures: List[str] = []

    actual = payload.get(
        "compartment_order",
        []
    )

    if actual != EXPECTED_COMPARTMENT_ORDER:

        failures.append(
            "invalid_compartment_order"
        )

    return failures


def validate_domain_registry(
    payload: Dict[str, Any]
) -> List[str]:

    failures: List[str] = []

    registry = payload.get(
        "domain_registry",
        []
    )

    if len(registry) != 5:

        failures.append(
            "invalid_primary_domain_count"
        )

    for domain in registry:

        if "domain" not in domain:

            failures.append(
                "missing_domain_name"
            )

        compartments = domain.get(
            "compartments",
            []
        )

        if len(compartments) != 4:

            failures.append(
                f"incomplete_compartments::{domain.get('domain')}"
            )

    return failures


def validate_overlays(
    payload: Dict[str, Any]
) -> List[str]:

    failures: List[str] = []

    overlays = payload.get(
        "cross_domain_overlay_layers",
        []
    )

    expected_routes = [
        "RBL | CONTRADICTION",
        "RBL | HISTORICAL_MEMORY",
        "RBL | FINAL_METRIC",
    ]

    actual_routes = [
        x["route"]
        for x in overlays
    ]

    for route in expected_routes:

        if route not in actual_routes:

            failures.append(
                f"missing_overlay::{route}"
            )

    return failures


def validate_embedded_layers(
    payload: Dict[str, Any]
) -> List[str]:

    failures: List[str] = []

    embedded = payload.get(
        "embedded_layers",
        []
    )

    if len(embedded) == 0:

        failures.append(
            "missing_embedded_layers"
        )

        return failures

    cb_routes = [
        x["route"]
        for x in embedded
    ]

    if "RBL | CB" not in cb_routes:

        failures.append(
            "missing_cb_embedded_layer"
        )

    return failures


def validate_parquet() -> List[str]:

    failures: List[str] = []

    if not INPUT_PARQUET.exists():

        failures.append(
            "missing_registry_parquet"
        )

        return failures

    try:

        df = pd.read_parquet(
            INPUT_PARQUET
        )

        if df.empty:

            failures.append(
                "empty_registry_parquet"
            )

    except Exception as e:

        failures.append(
            f"parquet_read_error::{e}"
        )

    return failures


def run_validation() -> pd.DataFrame:

    payload = load_json()

    failures: List[str] = []

    failures.extend(
        validate_required_keys(
            payload
        )
    )

    failures.extend(
        validate_domain_order(
            payload
        )
    )

    failures.extend(
        validate_compartment_order(
            payload
        )
    )

    failures.extend(
        validate_domain_registry(
            payload
        )
    )

    failures.extend(
        validate_overlays(
            payload
        )
    )

    failures.extend(
        validate_embedded_layers(
            payload
        )
    )

    failures.extend(
        validate_parquet()
    )

    return pd.DataFrame(
        [
            {
                "test": (
                    "oc_receiving_area_registry_v1"
                ),
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

    validation_df = (
        run_validation()
    )

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
        "OC Receiving Area Registry Validation v1"
    )

    print("-" * 60)

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
    
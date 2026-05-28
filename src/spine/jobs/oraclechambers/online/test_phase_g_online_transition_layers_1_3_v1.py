from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from oc_approved_data_connector_registry_v1 import (
    build_approved_data_connector_registry_v1,
)

from oc_online_runtime_gateway_v1 import (
    build_online_runtime_gateway_v1,
)

from oc_external_runtime_isolation_v1 import (
    build_external_runtime_isolation_v1,
)


def main() -> None:

    registry = build_approved_data_connector_registry_v1()

    gateway = build_online_runtime_gateway_v1()

    isolation = build_external_runtime_isolation_v1()

    failures: list[str] = []

    if not registry.get("connector_registry_ready"):
        failures.append("connector_registry_not_ready")

    if not gateway.get("gateway_ready"):
        failures.append("gateway_not_ready")

    if not isolation.get("runtime_isolation_ready"):
        failures.append("runtime_isolation_not_ready")

    if registry.get("online_transition_allowed"):
        failures.append("registry_online_gate_open")

    if gateway.get("online_transition_allowed"):
        failures.append("gateway_online_gate_open")

    if isolation.get("online_transition_allowed"):
        failures.append("isolation_online_gate_open")

    enabled_routes = [
        route for route in gateway.get("routes", [])
        if route["enabled"]
    ]

    if enabled_routes:
        failures.append("gateway_routes_should_be_disabled")

    result: dict[str, Any] = {
        "artifact": "test_phase_g_online_transition_layers_1_3_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "passed": len(failures) == 0,
        "failed_checks": failures,
        "validated_layers": [
            registry["artifact"],
            gateway["artifact"],
            isolation["artifact"],
        ],
    }

    print(result)

    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

    
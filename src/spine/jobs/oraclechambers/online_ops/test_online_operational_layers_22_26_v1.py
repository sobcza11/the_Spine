from datetime import datetime, timezone
from typing import Any

from oc_hybrid_failover_infrastructure_v1 import (
    build_hybrid_failover_infrastructure_v1
)

from oc_production_deployment_packaging_v1 import (
    build_production_deployment_packaging_v1
)

from oc_institutional_staging_certification_v1 import (
    build_institutional_staging_certification_v1
)

from oc_limited_institutional_online_operations_v1 import (
    build_limited_institutional_online_operations_v1
)

from oc_full_institutional_deployment_v1 import (
    build_full_institutional_deployment_v1
)


def main() -> None:

    layers = [
        build_hybrid_failover_infrastructure_v1(),
        build_production_deployment_packaging_v1(),
        build_institutional_staging_certification_v1(),
        build_limited_institutional_online_operations_v1(),
        build_full_institutional_deployment_v1(),
    ]

    failures: list[str] = []

    required_flags = [
        "hybrid_failover_ready",
        "deployment_packaging_ready",
        "staging_certification_ready",
        "limited_online_ops_ready",
        "institutional_deployment_ready",
    ]

    for layer, flag in zip(layers, required_flags):

        if not layer.get(flag):
            failures.append(f"{layer['artifact']}:{flag}:false")

        if layer.get("online_transition_allowed"):
            failures.append(f"{layer['artifact']}:online_gate_open")

    deployment = layers[-1]

    if deployment["deployment_tiers"]["institutional_online"]:
        failures.append("institutional_online_should_remain_disabled")

    result: dict[str, Any] = {
        "artifact": "test_online_operational_layers_22_26_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "passed": len(failures) == 0,
        "failed_checks": failures,
        "validated_layers": [layer["artifact"] for layer in layers],
    }

    print(result)

    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
    
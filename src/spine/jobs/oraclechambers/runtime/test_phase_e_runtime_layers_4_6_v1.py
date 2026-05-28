from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from oc_runtime_state_continuity_v1 import (
    build_runtime_state_continuity_v1,
)

from oc_runtime_observability_runtime_v1 import (
    build_runtime_observability_runtime_v1,
)

from oc_runtime_resiliency_v1 import (
    build_runtime_resiliency_v1,
)


def main() -> None:

    continuity = build_runtime_state_continuity_v1()

    observability = build_runtime_observability_runtime_v1()

    resiliency = build_runtime_resiliency_v1()

    failures: list[str] = []

    if not continuity.get("continuity_ready"):
        failures.append("continuity_not_ready")

    if not observability.get("observability_runtime_ready"):
        failures.append("observability_not_ready")

    if not resiliency.get("runtime_resiliency_ready"):
        failures.append("resiliency_not_ready")

    if continuity.get("online_transition_allowed"):
        failures.append("continuity_gate_open")

    if observability.get("online_transition_allowed"):
        failures.append("observability_gate_open")

    if resiliency.get("online_transition_allowed"):
        failures.append("resiliency_gate_open")

    result: dict[str, Any] = {
        "artifact": "test_phase_e_runtime_layers_4_6_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "passed": len(failures) == 0,
        "failed_checks": failures,
        "validated_layers": [
            continuity["artifact"],
            observability["artifact"],
            resiliency["artifact"],
        ],
    }

    print(result)

    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

    
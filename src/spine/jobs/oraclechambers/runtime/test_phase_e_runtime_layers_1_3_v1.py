from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from oc_runtime_event_bus_v1 import build_runtime_event_bus_v1
from oc_live_hydration_runtime_v1 import build_live_hydration_runtime_v1
from oc_streaming_cognition_layer_v1 import build_streaming_cognition_layer_v1


REPO_ROOT = Path(__file__).resolve().parents[5]


def main() -> None:
    os.chdir(REPO_ROOT)

    event_bus = build_runtime_event_bus_v1()
    hydration = build_live_hydration_runtime_v1()
    streaming = build_streaming_cognition_layer_v1()

    failures: list[str] = []

    if not event_bus.get("event_bus_ready"):
        failures.append("event_bus_not_ready")

    if not hydration.get("live_hydration_ready"):
        failures.append("live_hydration_not_ready")

    if not streaming.get("streaming_layer_ready"):
        failures.append("streaming_layer_not_ready")

    if event_bus.get("online_transition_allowed"):
        failures.append("event_bus_online_gate_open")

    if hydration.get("online_transition_allowed"):
        failures.append("hydration_online_gate_open")

    if streaming.get("online_transition_allowed"):
        failures.append("streaming_online_gate_open")

    result: dict[str, Any] = {
        "artifact": "test_phase_e_runtime_layers_1_3_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "repo_root": str(REPO_ROOT),
        "passed": len(failures) == 0,
        "failed_checks": failures,
        "validated_layers": [
            event_bus["artifact"],
            hydration["artifact"],
            streaming["artifact"],
        ],
    }

    print(result)

    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
    
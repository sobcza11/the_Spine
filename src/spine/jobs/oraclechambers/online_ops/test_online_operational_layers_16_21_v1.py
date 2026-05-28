from datetime import datetime, timezone
from typing import Any

from oc_hybrid_staging_runtime_v1 import build_hybrid_staging_runtime_v1
from oc_live_data_synchronization_v1 import build_live_data_synchronization_v1
from oc_online_runtime_orchestration_v1 import build_online_runtime_orchestration_v1
from oc_long_duration_runtime_stability_v1 import build_long_duration_runtime_stability_v1
from oc_institutional_monitoring_stack_v1 import build_institutional_monitoring_stack_v1
from oc_controlled_operator_console_v1 import build_controlled_operator_console_v1


def main() -> None:
    layers = [
        build_hybrid_staging_runtime_v1(),
        build_live_data_synchronization_v1(),
        build_online_runtime_orchestration_v1(),
        build_long_duration_runtime_stability_v1(),
        build_institutional_monitoring_stack_v1(),
        build_controlled_operator_console_v1(),
    ]

    failures: list[str] = []

    required_ready_flags = [
        "hybrid_staging_ready",
        "live_sync_ready",
        "online_orchestration_ready",
        "runtime_stability_ready",
        "monitoring_stack_ready",
        "operator_console_ready",
    ]

    for layer, ready_flag in zip(layers, required_ready_flags):
        if not layer.get(ready_flag):
            failures.append(f"{layer['artifact']}:{ready_flag}:false")

        if layer.get("online_transition_allowed"):
            failures.append(f"{layer['artifact']}:online_gate_open")

    sync = layers[1]
    if sync.get("connectors_online_enabled"):
        failures.append("connectors_should_remain_disabled")

    orchestration = layers[2]
    if orchestration.get("ai_orchestration_allowed"):
        failures.append("ai_orchestration_should_be_blocked")

    console = layers[5]
    if console["operator_actions"].get("approve_online_transition"):
        failures.append("operator_console_should_not_approve_online_yet")

    result: dict[str, Any] = {
        "artifact": "test_online_operational_layers_16_21_v1",
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

    
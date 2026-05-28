from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from oc_offline_segment_manifest_v1 import build_offline_segment_manifest_v1
from oc_segment_health_check_v1 import build_segment_health_check_v1
from oc_segment_routing_validation_v1 import build_segment_routing_validation_v1


def run_segmented_offline_deployment_test_v1() -> dict[str, Any]:
    manifest = build_offline_segment_manifest_v1()
    health = build_segment_health_check_v1()
    routing = build_segment_routing_validation_v1()

    passed = (
        bool(manifest["deployment_ready"])
        and bool(health["passed"])
        and bool(routing["passed"])
    )

    return {
        "artifact": "oc_segmented_offline_deployment_test_v1",
        "layer": "OracleChambers Segmented Offline Deployment Test",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "passed": passed,
        "offline_segment_strategy_validated": passed,
        "online_transition_allowed": False,
        "reason": (
            "Offline segmented sites validated. Online transition remains gated "
            "until each segment passes dedicated runtime, governance, security, "
            "and observability checks."
        ),
        "manifest": manifest,
        "health": health,
        "routing": routing,
    }


if __name__ == "__main__":
    output = run_segmented_offline_deployment_test_v1()

    for key, value in output.items():
        print(f"{key}: {value}")

        
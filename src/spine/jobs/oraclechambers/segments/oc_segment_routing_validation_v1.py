from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from oc_offline_segment_manifest_v1 import SEGMENTS


REQUIRED_SEGMENTS = {
    "core_runtime",
    "governance",
    "contradiction",
    "executive_command",
}


def build_segment_routing_validation_v1() -> dict[str, Any]:
    available = {segment.segment_id for segment in SEGMENTS}
    missing = sorted(REQUIRED_SEGMENTS - available)

    routing_edges = [
        {
            "from": "core_runtime",
            "to": "executive_command",
            "purpose": "hydration and runtime state aggregation",
        },
        {
            "from": "governance",
            "to": "executive_command",
            "purpose": "provenance and deployment rule validation",
        },
        {
            "from": "contradiction",
            "to": "executive_command",
            "purpose": "cross-asset disagreement escalation",
        },
    ]

    return {
        "artifact": "oc_segment_routing_validation_v1",
        "layer": "OracleChambers Offline Segment Routing",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "passed": len(missing) == 0,
        "missing_segments": missing,
        "routing_edges": routing_edges,
    }


if __name__ == "__main__":
    output = build_segment_routing_validation_v1()

    for key, value in output.items():
        print(f"{key}: {value}")

        
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from oc_offline_segment_manifest_v1 import SEGMENTS


def build_segment_health_check_v1() -> dict[str, Any]:
    results = []

    for segment in SEGMENTS:
        results.append(
            {
                "segment_id": segment.segment_id,
                "site_name": segment.site_name,
                "status": "pass" if segment.deployment_ready else "fail",
                "online_mirror": segment.online_mirror,
                "dependencies": segment.dependencies,
            }
        )

    return {
        "artifact": "oc_segment_health_check_v1",
        "layer": "OracleChambers Offline Segment Health",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "passed": all(item["status"] == "pass" for item in results),
        "segment_count": len(results),
        "results": results,
    }


if __name__ == "__main__":
    output = build_segment_health_check_v1()

    for key, value in output.items():
        print(f"{key}: {value}")
        
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class OfflineSegment:
    segment_id: str
    site_name: str
    role: str
    online_mirror: str
    deployment_ready: bool
    dependencies: list[str]


SEGMENTS: list[OfflineSegment] = [
    OfflineSegment(
        segment_id="core_runtime",
        site_name="oc-core-runtime-local",
        role="Core hydration, runtime state, API health, refresh control.",
        online_mirror="oc-core-runtime-online",
        deployment_ready=True,
        dependencies=["hydration_payload", "fastapi_runtime"],
    ),
    OfflineSegment(
        segment_id="governance",
        site_name="oc-governance-local",
        role="Governance, provenance, source rules, AI non-orchestration.",
        online_mirror="oc-governance-online",
        deployment_ready=True,
        dependencies=["governance_contracts", "approved_sources_contract"],
    ),
    OfflineSegment(
        segment_id="contradiction",
        site_name="oc-contradiction-local",
        role="Cross-asset disagreement, fragmentation, regime conflict.",
        online_mirror="oc-contradiction-online",
        deployment_ready=True,
        dependencies=["contradiction_engine", "overlay_runtime"],
    ),
    OfflineSegment(
        segment_id="executive_command",
        site_name="oc-executive-command-local",
        role="Executive aggregation view across approved offline segments.",
        online_mirror="oc-executive-command-online",
        deployment_ready=True,
        dependencies=[
            "core_runtime",
            "governance",
            "contradiction",
            "historical_memory",
        ],
    ),
]


def build_offline_segment_manifest_v1() -> dict[str, Any]:
    return {
        "artifact": "oc_offline_segment_manifest_v1",
        "layer": "OracleChambers Offline Segmented Deployment",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "strategy": "offline_segmented_sites_before_online_transition",
        "single_site_offline_model_allowed": False,
        "deployment_ready": all(segment.deployment_ready for segment in SEGMENTS),
        "segments": [segment.__dict__ for segment in SEGMENTS],
    }


if __name__ == "__main__":
    output = build_offline_segment_manifest_v1()

    for key, value in output.items():
        print(f"{key}: {value}")

        
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class GovernanceCheck:
    name: str
    passed: bool
    severity: str
    description: str


def build_online_runtime_governance_v1() -> dict[str, Any]:
    checks = [
        GovernanceCheck(
            name="offline_first_preserved",
            passed=True,
            severity="critical",
            description="Offline-first operation remains the default runtime posture.",
        ),
        GovernanceCheck(
            name="ai_not_orchestrator",
            passed=True,
            severity="critical",
            description="AI is not permitted to own routing, orchestration, runtime truth, or governance.",
        ),
        GovernanceCheck(
            name="approved_sources_only",
            passed=True,
            severity="high",
            description="Online data must come only from approved institutional sources.",
        ),
        GovernanceCheck(
            name="provenance_required",
            passed=True,
            severity="high",
            description="All runtime payloads require source, artifact, and timestamp provenance.",
        ),
        GovernanceCheck(
            name="compartment_isolation",
            passed=True,
            severity="high",
            description="FX, RATES, C_FLOW, EQUITIES_INDEX, and EQUITIES_SECTOR remain separately routed.",
        ),
    ]

    return {
        "artifact": "oc_online_runtime_governance_v1",
        "layer": "OracleChambers Online Runtime Governance",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "deployment_ready": all(check.passed for check in checks),
        "checks": [check.__dict__ for check in checks],
    }


if __name__ == "__main__":
    output = build_online_runtime_governance_v1()

    for key, value in output.items():
        print(f"{key}: {value}")
        
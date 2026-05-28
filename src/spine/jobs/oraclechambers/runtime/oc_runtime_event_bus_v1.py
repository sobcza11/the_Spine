from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4


@dataclass(frozen=True)
class RuntimeEvent:
    event_id: str
    event_type: str
    source_plane: str
    target_plane: str
    severity: str
    payload: dict[str, Any]
    ts: str


def build_runtime_event_bus_v1() -> dict[str, Any]:
    events = [
        RuntimeEvent(
            event_id=str(uuid4()),
            event_type="hydration_refresh",
            source_plane="oc-core-runtime-local",
            target_plane="oc-executive-dashboard-local",
            severity="medium",
            payload={
                "reason": "Refresh executive dashboard from governed hydration state.",
                "online_transition_allowed": False,
            },
            ts=datetime.now(timezone.utc).isoformat(),
        ),
        RuntimeEvent(
            event_id=str(uuid4()),
            event_type="contradiction_escalation",
            source_plane="oc-contradiction-matrix-local",
            target_plane="oc-executive-dashboard-local",
            severity="high",
            payload={
                "reason": "Cross-domain contradiction topology requires executive review.",
                "focus": "FX-RATES / RATES-EQUITIES_INDEX",
            },
            ts=datetime.now(timezone.utc).isoformat(),
        ),
        RuntimeEvent(
            event_id=str(uuid4()),
            event_type="historical_context_update",
            source_plane="oc-regime-timeline-local",
            target_plane="oc-executive-dashboard-local",
            severity="medium",
            payload={
                "top_match": "1998 FX Contagion",
                "similarity_score": 0.65,
            },
            ts=datetime.now(timezone.utc).isoformat(),
        ),
    ]

    return {
        "artifact": "oc_runtime_event_bus_v1",
        "layer": "OracleChambers Runtime Event Bus",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "event_bus_ready": True,
        "online_transition_allowed": False,
        "events": [event.__dict__ for event in events],
    }


if __name__ == "__main__":
    output = build_runtime_event_bus_v1()

    for key, value in output.items():
        print(f"{key}: {value}")

        
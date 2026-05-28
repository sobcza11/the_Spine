from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[5]

SOURCE_PAYLOAD = (
    REPO_ROOT
    / "data"
    / "serving"
    / "oraclechambers"
    / "oc_local_site_hydration_v1.json"
)


def build_live_hydration_runtime_v1() -> dict[str, Any]:
    payload = json.loads(SOURCE_PAYLOAD.read_text(encoding="utf-8"))

    site_payload = payload.get("site_payload", {})
    headline = site_payload.get("headline", {})
    routing = payload.get("routing", {})

    return {
        "artifact": "oc_live_hydration_runtime_v1",
        "layer": "OracleChambers Live Hydration Runtime",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "live_hydration_ready": True,
        "online_transition_allowed": False,
        "source_payload": str(SOURCE_PAYLOAD),
        "current_state": {
            "deployment_ready": payload.get("deployment_ready"),
            "site_mode": site_payload.get("site_mode"),
            "runtime_mode": site_payload.get("runtime_mode"),
            "regime": headline.get("regime"),
            "confidence": headline.get("confidence"),
            "conviction": headline.get("conviction"),
            "macro_temperature": headline.get("macro_temperature"),
            "frontend_hydration_ready": routing.get("frontend_hydration_ready"),
            "executive_dashboard_ready": routing.get("executive_dashboard_ready"),
            "ai_dependency": routing.get("ai_dependency"),
        },
        "mutation_policy": {
            "deterministic_only": True,
            "ai_may_not_mutate_runtime_truth": True,
            "online_updates_blocked_until_gate_open": True,
        },
    }


if __name__ == "__main__":
    output = build_live_hydration_runtime_v1()

    for key, value in output.items():
        print(f"{key}: {value}")

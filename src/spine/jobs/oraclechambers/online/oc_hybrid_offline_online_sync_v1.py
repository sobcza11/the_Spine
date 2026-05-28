from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import json


REPO_ROOT = Path(__file__).resolve().parents[5]

OFFLINE_PAYLOAD = (
    REPO_ROOT
    / "data"
    / "serving"
    / "oraclechambers"
    / "oc_local_site_hydration_v1.json"
)


def build_hybrid_offline_online_sync_v1() -> dict[str, Any]:
    offline_payload = json.loads(
        OFFLINE_PAYLOAD.read_text(encoding="utf-8")
    )

    site_payload = offline_payload.get("site_payload", {})
    headline = site_payload.get("headline", {})
    routing = offline_payload.get("routing", {})

    online_mirror_state = {
        "mirror_enabled": False,
        "mirror_mode": "not_connected",
        "online_transition_allowed": False,
        "reason": "Online mirror remains disabled until executive gate approval.",
    }

    sync_comparison = {
        "offline_regime": headline.get("regime"),
        "offline_confidence": headline.get("confidence"),
        "offline_runtime_mode": site_payload.get("runtime_mode"),
        "offline_ready": routing.get("offline_first"),
        "online_mirror_enabled": online_mirror_state["mirror_enabled"],
        "state_match_required_before_activation": True,
    }

    return {
        "artifact": "oc_hybrid_offline_online_sync_v1",
        "layer": "OracleChambers Hybrid Offline/Online Sync",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "hybrid_sync_ready": True,
        "online_transition_allowed": False,
        "offline_payload": str(OFFLINE_PAYLOAD),
        "online_mirror_state": online_mirror_state,
        "sync_comparison": sync_comparison,
        "activation_rules": {
            "offline_state_must_validate": True,
            "online_mirror_must_match_offline": True,
            "executive_gate_required": True,
            "ai_may_not_authorize_sync": True,
        },
    }


if __name__ == "__main__":
    print(build_hybrid_offline_online_sync_v1())
    
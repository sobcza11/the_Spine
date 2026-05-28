from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import json


REPO_ROOT = Path(__file__).resolve().parents[5]
OFFLINE_PAYLOAD = REPO_ROOT / "data" / "serving" / "oraclechambers" / "oc_local_site_hydration_v1.json"


def build_online_synchronization_testing_v1() -> dict[str, Any]:
    payload_exists = OFFLINE_PAYLOAD.exists()

    offline_state = {}

    if payload_exists:
        payload = json.loads(OFFLINE_PAYLOAD.read_text(encoding="utf-8"))
        headline = payload.get("site_payload", {}).get("headline", {})
        offline_state = {
            "regime": headline.get("regime"),
            "confidence": headline.get("confidence"),
            "macro_temperature": headline.get("macro_temperature"),
        }

    mirror_state = {
        "mirror_connected": False,
        "mirror_state_loaded": False,
        "state_match": False,
    }

    return {
        "artifact": "oc_online_synchronization_testing_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "sync_test_ready": payload_exists,
        "online_transition_allowed": False,
        "offline_state": offline_state,
        "mirror_state": mirror_state,
        "sync_passed": False,
        "reason": "Online mirror is not connected yet.",
    }


if __name__ == "__main__":
    print(build_online_synchronization_testing_v1())

    
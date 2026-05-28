from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter

router = APIRouter(prefix="/oc/events", tags=["runtime-events"])


@router.get("/heartbeat")
def heartbeat() -> dict[str, Any]:
    return {
        "event": "heartbeat",
        "status": "active",
        "runtime_mode": "local_runtime",
        "ts": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/refresh")
def refresh_event() -> dict[str, Any]:
    return {
        "event": "refresh_requested",
        "status": "accepted",
        "scope": "hydration_payload",
        "ts": datetime.now(timezone.utc).isoformat(),
    }

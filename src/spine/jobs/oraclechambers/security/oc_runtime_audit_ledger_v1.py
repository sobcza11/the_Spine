from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import json


REPO_ROOT = Path(__file__).resolve().parents[5]

AUDIT_DIR = (
    REPO_ROOT
    / "data"
    / "audit"
    / "oraclechambers"
)

AUDIT_DIR.mkdir(parents=True, exist_ok=True)

AUDIT_FILE = AUDIT_DIR / "oc_runtime_audit_ledger_v1.jsonl"


def append_audit_event(
    event_type: str,
    actor_role: str,
    action: str,
    allowed: bool,
    reason: str,
) -> dict[str, Any]:
    event = {
        "event_id": str(uuid4()),
        "artifact": "oc_runtime_audit_event_v1",
        "ts": datetime.now(timezone.utc).isoformat(),
        "event_type": event_type,
        "actor_role": actor_role,
        "action": action,
        "allowed": allowed,
        "reason": reason,
    }

    with AUDIT_FILE.open("a", encoding="utf-8") as file:
        file.write(json.dumps(event) + "\n")

    return event


def build_runtime_audit_ledger_v1() -> dict[str, Any]:
    seed_event = append_audit_event(
        event_type="audit_ledger_initialized",
        actor_role="system",
        action="initialize_runtime_audit_ledger",
        allowed=True,
        reason="Audit ledger initialized for institutional runtime traceability.",
    )

    return {
        "artifact": "oc_runtime_audit_ledger_v1",
        "layer": "OracleChambers Runtime Audit Ledger",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "audit_ledger_ready": True,
        "audit_file": str(AUDIT_FILE),
        "seed_event": seed_event,
    }


if __name__ == "__main__":
    print(build_runtime_audit_ledger_v1())

    
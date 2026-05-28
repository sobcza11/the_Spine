from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import json


REPO_ROOT = Path(__file__).resolve().parents[5]

HYDRATION_PAYLOAD = (
    REPO_ROOT
    / "data"
    / "serving"
    / "oraclechambers"
    / "oc_local_site_hydration_v1.json"
)

AUDIT_LEDGER = (
    REPO_ROOT
    / "data"
    / "audit"
    / "oraclechambers"
    / "oc_runtime_audit_ledger_v1.jsonl"
)


def build_compliance_provenance_layer_v1() -> dict[str, Any]:
    payload_exists = HYDRATION_PAYLOAD.exists()
    audit_exists = AUDIT_LEDGER.exists()

    provenance = {}

    if payload_exists:
        payload = json.loads(
            HYDRATION_PAYLOAD.read_text(encoding="utf-8")
        )
        provenance = payload.get("provenance", {})

    return {
        "artifact": "oc_compliance_provenance_layer_v1",
        "layer": "OracleChambers Compliance & Provenance",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "compliance_provenance_ready": payload_exists and audit_exists,
        "online_transition_allowed": False,
        "cpmai_alignment": {
            "deterministic_routing": True,
            "ai_non_orchestrating": True,
            "auditability": audit_exists,
            "provenance_available": bool(provenance),
            "offline_first": True,
        },
        "source_paths": {
            "hydration_payload": str(HYDRATION_PAYLOAD),
            "audit_ledger": str(AUDIT_LEDGER),
        },
        "payload_provenance": provenance,
    }


if __name__ == "__main__":
    print(build_compliance_provenance_layer_v1())

    
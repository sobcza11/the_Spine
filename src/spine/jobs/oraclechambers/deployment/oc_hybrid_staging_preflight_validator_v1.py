from datetime import datetime, timezone
from typing import Any


def build_hybrid_staging_preflight_validator_v1() -> dict[str, Any]:
    checks = {
        "offline_rc_certified": True,
        "security_governance_ready": True,
        "runtime_standardization_ready": True,
        "audit_ledger_ready": True,
        "operator_approval_record_exists": False,
        "connector_credentials_loaded": False,
        "online_transition_allowed": False,
    }

    blocking_items = [
        k for k, v in checks.items()
        if k in {"operator_approval_record_exists", "connector_credentials_loaded"}
        and not v
    ]

    return {
        "artifact": "oc_hybrid_staging_preflight_validator_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "hybrid_preflight_ready": len(blocking_items) == 0,
        "online_transition_allowed": False,
        "blocking_items": blocking_items,
        "checks": checks,
    }


if __name__ == "__main__":
    print(build_hybrid_staging_preflight_validator_v1())

    
"""Deterministic executive status for the governed EQUITIES release."""

from __future__ import annotations

import json
from typing import Any, Callable

from spine.jobs.equities.audit_lens_governance_evidence_v1 import (
    audit_lens_governance_evidence,
)

EXPECTED_LENSES = {
    "MARKET_BREADTH",
    "VOLATILITY_STRUCTURE",
    "LIQUIDITY_FLOWS",
}


def report_equities_governance_status(
    auditor: Callable[[], list[dict[str, Any]]] = audit_lens_governance_evidence,
) -> dict[str, Any]:
    blocking: set[str] = set()
    runtime_state = "VALIDATION_ONLY"
    activation_state = "PROHIBITED"
    try:
        audits = auditor()
    except Exception:
        audits = []
        blocking.add("LENS_GOVERNANCE_AUDIT_UNAVAILABLE")

    lens_ids = {
        result.get("lens_id")
        for result in audits
        if isinstance(result, dict)
    }
    if lens_ids != EXPECTED_LENSES:
        blocking.add("LENS_GOVERNANCE_SCOPE_INCOMPLETE")

    for result in audits:
        lens_id = str(result.get("lens_id", "UNKNOWN"))
        if result.get("overall_status") != "READY":
            blocking.add(f"{lens_id}:GOVERNANCE_BLOCKED")
        for item in result.get("missing_requirements", []):
            blocking.add(f"{lens_id}:{item}")
        if result.get("runtime_state") != "VALIDATION_ONLY":
            blocking.add(f"{lens_id}:RUNTIME_STATE_INVALID")
        if result.get("activation_state") != "PROHIBITED":
            blocking.add(f"{lens_id}:ACTIVATION_STATE_INVALID")

    lens_ready = bool(audits) and not blocking and all(
        result.get("overall_status") == "READY" for result in audits
    )
    return {
        "equities_status": (
            "GOVERNANCE_READY_FOR_SEPARATE_ACTIVATION_REVIEW"
            if lens_ready
            else "IN_PROGRESS"
        ),
        "qqq_status": (
            "SERVING_COMPLETE_LENS_GOVERNANCE_READY"
            if lens_ready
            else "SERVING_COMPLETE_LENS_BLOCKED"
        ),
        "lens_status": "READY_FOR_SEPARATE_ACTIVATION_REVIEW" if lens_ready else "BLOCKED",
        "blocking_items": sorted(blocking),
        "runtime_state": runtime_state,
        "activation_state": activation_state,
    }


def main() -> int:
    print(json.dumps(
        report_equities_governance_status(),
        indent=2,
        sort_keys=True,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

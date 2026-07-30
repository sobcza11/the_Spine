"""Fail-closed aggregate audit of EQUITIES lens governance evidence."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from spine.jobs.equities.check_lens_contract_readiness_v1 import (
    CONTRACT_DIR,
    EVIDENCE_DIR,
    EVIDENCE_FILES,
    POLICY_DIR,
    POLICY_FILES,
    check_evidence,
    check_lens_contract_readiness,
    check_policy,
)


def _aggregate_status(values: list[str], ready: str, incomplete: str) -> str:
    return ready if values and all(value == ready for value in values) else incomplete


def audit_lens_governance_evidence(
    *,
    contract_dir: Path = CONTRACT_DIR,
    policy_dir: Path = POLICY_DIR,
    evidence_dir: Path = EVIDENCE_DIR,
) -> list[dict[str, Any]]:
    readiness = check_lens_contract_readiness(
        contract_dir,
        policy_dir,
        evidence_dir,
    )
    policy_results = {
        name: check_policy(policy_dir / filename)
        for name, filename in POLICY_FILES.items()
    }
    evidence_results = {
        name: check_evidence(evidence_dir / filename)
        for name, filename in EVIDENCE_FILES.items()
    }
    policy_status = _aggregate_status(
        [result["status"] for result in policy_results.values()],
        "READY",
        "POLICY_INCOMPLETE",
    )
    evidence_status = _aggregate_status(
        [result["status"] for result in evidence_results.values()],
        "READY",
        "EVIDENCE_INCOMPLETE",
    )

    audited: list[dict[str, Any]] = []
    for result in readiness:
        missing = set(result["missing_governance_items"])
        for name, policy in policy_results.items():
            missing.update(f"{name.upper()}:{code}" for code in policy["reason_codes"])
        for name, evidence in evidence_results.items():
            missing.update(f"{name.upper()}:{code}" for code in evidence["reason_codes"])
        if result["runtime_state"] != "VALIDATION_ONLY":
            missing.add("RUNTIME_STATE_NOT_VALIDATION_ONLY")
        if result["activation_state"] != "PROHIBITED":
            missing.add("ACTIVATION_STATE_NOT_PROHIBITED")
        contract_status = result["status"]
        overall = (
            "READY"
            if (
                contract_status == "READY"
                and policy_status == "READY"
                and evidence_status == "READY"
                and result["runtime_state"] == "VALIDATION_ONLY"
                and result["activation_state"] == "PROHIBITED"
                and not missing
            )
            else "BLOCKED"
        )
        audited.append({
            "lens_id": result["lens_id"],
            "contract_status": contract_status,
            "policy_status": policy_status,
            "evidence_status": evidence_status,
            "missing_requirements": sorted(missing),
            "runtime_state": result["runtime_state"],
            "activation_state": result["activation_state"],
            "overall_status": overall,
        })
    return audited


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--contract-dir", type=Path, default=CONTRACT_DIR)
    parser.add_argument("--policy-dir", type=Path, default=POLICY_DIR)
    parser.add_argument("--evidence-dir", type=Path, default=EVIDENCE_DIR)
    args = parser.parse_args()
    print(json.dumps(
        audit_lens_governance_evidence(
            contract_dir=args.contract_dir,
            policy_dir=args.policy_dir,
            evidence_dir=args.evidence_dir,
        ),
        indent=2,
        sort_keys=True,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

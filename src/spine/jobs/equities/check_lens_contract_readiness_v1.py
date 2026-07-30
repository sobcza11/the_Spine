"""Validate governed EQUITIES lens input-contract readiness without activation."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

from spine.equities.index_profiles import REPO_ROOT

CONTRACT_DIR = REPO_ROOT / "governance/equities/lens_contracts"
POLICY_DIR = REPO_ROOT / "governance/equities/lens_policies"
EVIDENCE_DIR = REPO_ROOT / "governance/equities/lens_evidence"
CONTRACT_FILES = {
    "MARKET_BREADTH": "EQUITIES_MARKET_BREADTH_INPUT_CONTRACT_V1.md",
    "VOLATILITY_STRUCTURE": "EQUITIES_VOLATILITY_STRUCTURE_INPUT_CONTRACT_V1.md",
    "LIQUIDITY_FLOWS": "EQUITIES_LIQUIDITY_FLOW_INPUT_CONTRACT_V1.md",
}
REQUIRED_KEYS = {
    "lens_id",
    "contract_version",
    "required_inputs",
    "required_fields",
    "observation_key",
    "history_policy",
    "missing_observation_policy",
    "universe_policy",
    "runtime_state",
    "activation_state",
}
PENDING_VALUES = {
    "OBSERVATION_KEY_PENDING": "OBSERVATION_KEY_PENDING",
    "HISTORY_REQUIREMENT_PENDING": "HISTORY_REQUIREMENT_PENDING",
    "MISSING_OBSERVATION_POLICY_PENDING": "MISSING_OBSERVATION_POLICY_PENDING",
    "BREADTH_UNIVERSE_CONTRACT_PENDING": "UNIVERSE_REQUIREMENT_PENDING",
    "UNIVERSE_REQUIREMENT_PENDING": "UNIVERSE_REQUIREMENT_PENDING",
}
CONTRACT_PATTERN = re.compile(
    r"<!-- LENS_CONTRACT_JSON_V1 -->\s*```json\s*(\{.*?\})\s*```",
    re.DOTALL,
)
POLICY_PATTERN = re.compile(
    r"<!-- LENS_POLICY_JSON_V1 -->\s*```json\s*(\{.*?\})\s*```",
    re.DOTALL,
)
POLICY_FILES = {
    "history_policy_status": "EQUITIES_LENS_HISTORY_POLICY_V1.md",
    "missing_observation_policy_status": "EQUITIES_LENS_MISSING_OBSERVATION_POLICY_V1.md",
    "universe_policy_status": "EQUITIES_LENS_UNIVERSE_POLICY_V1.md",
}
EVIDENCE_FILES = {
    "history_evidence_status": "EQUITIES_LENS_HISTORY_EVIDENCE_BINDING_V1.md",
    "missing_observation_evidence_status": "EQUITIES_LENS_MISSING_OBSERVATION_EVIDENCE_BINDING_V1.md",
    "universe_evidence_status": "EQUITIES_LENS_UNIVERSE_EVIDENCE_BINDING_V1.md",
}
EVIDENCE_PATTERN = re.compile(
    r"<!-- LENS_EVIDENCE_JSON_V1 -->\s*```json\s*(\{.*?\})\s*```",
    re.DOTALL,
)


def load_contract(path: Path) -> dict[str, Any]:
    match = CONTRACT_PATTERN.search(path.read_text(encoding="utf-8"))
    if not match:
        raise ValueError("LENS_CONTRACT_MACHINE_BLOCK_MISSING")
    value = json.loads(match.group(1))
    if not isinstance(value, dict):
        raise ValueError("LENS_CONTRACT_OBJECT_REQUIRED")
    return value


def check_policy(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"status": "POLICY_INCOMPLETE", "reason_codes": ["POLICY_MISSING"]}
    try:
        match = POLICY_PATTERN.search(path.read_text(encoding="utf-8"))
        if not match:
            raise ValueError("LENS_POLICY_MACHINE_BLOCK_MISSING")
        policy = json.loads(match.group(1))
        if not isinstance(policy, dict):
            raise ValueError("LENS_POLICY_OBJECT_REQUIRED")
    except (OSError, ValueError, json.JSONDecodeError):
        return {"status": "POLICY_INCOMPLETE", "reason_codes": ["POLICY_MALFORMED"]}
    reasons: set[str] = set()
    if not policy.get("policy_id") or not policy.get("policy_version"):
        reasons.add("POLICY_IDENTITY_INCOMPLETE")
    if policy.get("runtime_state") != "VALIDATION_ONLY":
        reasons.add("POLICY_RUNTIME_NOT_VALIDATION_ONLY")
    if policy.get("activation_state") != "PROHIBITED":
        reasons.add("POLICY_ACTIVATION_NOT_PROHIBITED")
    if policy.get("policy_status") != "READY":
        reasons.add("POLICY_STATUS_INCOMPLETE")
    return {
        "status": "READY" if not reasons else "POLICY_INCOMPLETE",
        "reason_codes": sorted(reasons),
    }


def check_evidence(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"status": "EVIDENCE_INCOMPLETE", "reason_codes": ["EVIDENCE_MISSING"]}
    try:
        match = EVIDENCE_PATTERN.search(path.read_text(encoding="utf-8"))
        if not match:
            raise ValueError("LENS_EVIDENCE_MACHINE_BLOCK_MISSING")
        evidence = json.loads(match.group(1))
        if not isinstance(evidence, dict):
            raise ValueError("LENS_EVIDENCE_OBJECT_REQUIRED")
    except (OSError, ValueError, json.JSONDecodeError):
        return {"status": "EVIDENCE_INCOMPLETE", "reason_codes": ["EVIDENCE_MALFORMED"]}
    reasons: set[str] = set()
    if not evidence.get("evidence_id") or not evidence.get("evidence_version"):
        reasons.add("EVIDENCE_IDENTITY_INCOMPLETE")
    if evidence.get("runtime_state") != "VALIDATION_ONLY":
        reasons.add("EVIDENCE_RUNTIME_NOT_VALIDATION_ONLY")
    if evidence.get("activation_state") != "PROHIBITED":
        reasons.add("EVIDENCE_ACTIVATION_NOT_PROHIBITED")
    if evidence.get("evidence_status") != "READY":
        reasons.add("EVIDENCE_STATUS_INCOMPLETE")
    return {
        "status": "READY" if not reasons else "EVIDENCE_INCOMPLETE",
        "reason_codes": sorted(reasons),
    }


def check_contract(
    lens_id: str,
    path: Path,
    policy_statuses: dict[str, str] | None = None,
    evidence_statuses: dict[str, str] | None = None,
) -> dict[str, Any]:
    policy_statuses = policy_statuses or {
        name: "POLICY_INCOMPLETE" for name in POLICY_FILES
    }
    evidence_statuses = evidence_statuses or {
        name: "EVIDENCE_INCOMPLETE" for name in EVIDENCE_FILES
    }
    missing: set[str] = set()
    if not path.exists():
        return {
            "lens_id": lens_id,
            "contract_version": None,
            "required_inputs": [],
            "missing_governance_items": ["CONTRACT_MISSING"],
            "runtime_state": None,
            "activation_state": None,
            "status": "CONTRACT_INCOMPLETE",
            **policy_statuses,
            **evidence_statuses,
        }
    try:
        contract = load_contract(path)
    except (OSError, ValueError, json.JSONDecodeError):
        return {
            "lens_id": lens_id,
            "contract_version": None,
            "required_inputs": [],
            "missing_governance_items": ["CONTRACT_MALFORMED"],
            "runtime_state": None,
            "activation_state": None,
            "status": "CONTRACT_INCOMPLETE",
            **policy_statuses,
            **evidence_statuses,
        }

    for key in sorted(REQUIRED_KEYS - set(contract)):
        missing.add(f"REQUIRED_ITEM_MISSING:{key}")
    if contract.get("lens_id") != lens_id:
        missing.add("LENS_ID_MISMATCH")
    if not contract.get("contract_version"):
        missing.add("CONTRACT_VERSION_MISSING")
    if not isinstance(contract.get("required_inputs"), list) or not contract.get("required_inputs"):
        missing.add("REQUIRED_INPUTS_MISSING")
    if contract.get("runtime_state") != "VALIDATION_ONLY":
        missing.add("RUNTIME_STATE_NOT_VALIDATION_ONLY")
    if contract.get("activation_state") != "PROHIBITED":
        missing.add("ACTIVATION_STATE_NOT_PROHIBITED")
    policy_values = (
        contract.get("observation_key"),
        contract.get("history_policy"),
        contract.get("missing_observation_policy"),
        contract.get("universe_policy"),
    )
    for value, reason in PENDING_VALUES.items():
        if any(candidate == value for candidate in policy_values):
            missing.add(reason)
    return {
        "lens_id": lens_id,
        "contract_version": contract.get("contract_version"),
        "required_inputs": contract.get("required_inputs", []),
        "missing_governance_items": sorted(missing),
        "runtime_state": contract.get("runtime_state"),
        "activation_state": contract.get("activation_state"),
        "status": "READY" if not missing else "CONTRACT_INCOMPLETE",
        **policy_statuses,
        **evidence_statuses,
    }


def check_lens_contract_readiness(
    contract_dir: Path = CONTRACT_DIR,
    policy_dir: Path = POLICY_DIR,
    evidence_dir: Path = EVIDENCE_DIR,
) -> list[dict[str, Any]]:
    policy_statuses = {
        field: check_policy(policy_dir / filename)["status"]
        for field, filename in POLICY_FILES.items()
    }
    evidence_statuses = {
        field: check_evidence(evidence_dir / filename)["status"]
        for field, filename in EVIDENCE_FILES.items()
    }
    return [
        check_contract(
            lens_id,
            contract_dir / filename,
            policy_statuses,
            evidence_statuses,
        )
        for lens_id, filename in CONTRACT_FILES.items()
    ]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--contract-dir", type=Path, default=CONTRACT_DIR)
    parser.add_argument("--policy-dir", type=Path, default=POLICY_DIR)
    parser.add_argument("--evidence-dir", type=Path, default=EVIDENCE_DIR)
    args = parser.parse_args()
    print(json.dumps(
        check_lens_contract_readiness(
            args.contract_dir,
            args.policy_dir,
            args.evidence_dir,
        ),
        indent=2,
        sort_keys=True,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

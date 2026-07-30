import json
from pathlib import Path

from spine.jobs.equities.check_lens_contract_readiness_v1 import (
    CONTRACT_FILES,
    check_contract,
    check_lens_contract_readiness,
)


def _write_contract(path: Path, value: dict):
    path.write_text(
        "# Test\n\n<!-- LENS_CONTRACT_JSON_V1 -->\n```json\n"
        + json.dumps(value, sort_keys=True)
        + "\n```\n",
        encoding="utf-8",
    )


def _complete(lens_id="VOLATILITY_STRUCTURE"):
    return {
        "lens_id": lens_id,
        "contract_version": "1.0.0",
        "required_inputs": ["GOVERNED_INPUT"],
        "required_fields": ["date", "value"],
        "observation_key": ["instrument_id", "date"],
        "history_policy": "GOVERNED_HISTORY_POLICY_V1",
        "missing_observation_policy": "FAIL_CLOSED_V1",
        "universe_policy": "SINGLE_INSTRUMENT_V1",
        "runtime_state": "VALIDATION_ONLY",
        "activation_state": "PROHIBITED",
    }


def test_missing_contract(tmp_path):
    result = check_contract("MARKET_BREADTH", tmp_path / "missing.md")
    assert result["status"] == "CONTRACT_INCOMPLETE"
    assert result["missing_governance_items"] == ["CONTRACT_MISSING"]


def test_malformed_contract(tmp_path):
    path = tmp_path / "malformed.md"
    path.write_text("# no machine contract", encoding="utf-8")
    result = check_contract("MARKET_BREADTH", path)
    assert result["missing_governance_items"] == ["CONTRACT_MALFORMED"]


def test_incomplete_history_policy(tmp_path):
    path = tmp_path / "contract.md"
    value = _complete()
    value["history_policy"] = "HISTORY_REQUIREMENT_PENDING"
    _write_contract(path, value)
    result = check_contract("VOLATILITY_STRUCTURE", path)
    assert result["status"] == "CONTRACT_INCOMPLETE"
    assert "HISTORY_REQUIREMENT_PENDING" in result["missing_governance_items"]


def test_incomplete_universe_policy(tmp_path):
    path = tmp_path / "contract.md"
    value = _complete("MARKET_BREADTH")
    value["universe_policy"] = "BREADTH_UNIVERSE_CONTRACT_PENDING"
    _write_contract(path, value)
    result = check_contract("MARKET_BREADTH", path)
    assert "UNIVERSE_REQUIREMENT_PENDING" in result["missing_governance_items"]


def test_deterministic_output():
    assert check_lens_contract_readiness() == check_lens_contract_readiness()


def test_validation_only_and_activation_enforcement(tmp_path):
    path = tmp_path / "contract.md"
    value = _complete()
    value["runtime_state"] = "PRODUCTION"
    value["activation_state"] = "ACTIVE"
    _write_contract(path, value)
    result = check_contract("VOLATILITY_STRUCTURE", path)
    assert "RUNTIME_STATE_NOT_VALIDATION_ONLY" in result["missing_governance_items"]
    assert "ACTIVATION_STATE_NOT_PROHIBITED" in result["missing_governance_items"]


def test_repository_contracts_exist_and_fail_closed_on_pending_items():
    results = check_lens_contract_readiness()
    assert [result["lens_id"] for result in results] == list(CONTRACT_FILES)
    assert all(result["status"] == "CONTRACT_INCOMPLETE" for result in results)
    assert all(result["runtime_state"] == "VALIDATION_ONLY" for result in results)
    assert all(result["activation_state"] == "PROHIBITED" for result in results)

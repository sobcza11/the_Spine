import json
from pathlib import Path

from spine.jobs.equities.check_lens_contract_readiness_v1 import (
    POLICY_DIR,
    POLICY_FILES,
    check_lens_contract_readiness,
    check_policy,
)


def _write_policy(path: Path, value: dict):
    path.write_text(
        "# Policy\n\n<!-- LENS_POLICY_JSON_V1 -->\n```json\n"
        + json.dumps(value, sort_keys=True)
        + "\n```\n",
        encoding="utf-8",
    )


def _policy():
    return {
        "policy_id": "TEST_POLICY_V1",
        "policy_version": "1.0.0",
        "policy_status": "READY",
        "runtime_state": "VALIDATION_ONLY",
        "activation_state": "PROHIBITED",
    }


def test_missing_policy_detection(tmp_path):
    result = check_policy(tmp_path / "missing.md")
    assert result == {
        "status": "POLICY_INCOMPLETE",
        "reason_codes": ["POLICY_MISSING"],
    }


def test_malformed_policy_rejection(tmp_path):
    path = tmp_path / "malformed.md"
    path.write_text("# missing machine block", encoding="utf-8")
    assert check_policy(path)["reason_codes"] == ["POLICY_MALFORMED"]


def test_policy_output_is_deterministic():
    assert check_lens_contract_readiness() == check_lens_contract_readiness()


def test_validation_only_enforcement(tmp_path):
    path = tmp_path / "policy.md"
    value = _policy()
    value["runtime_state"] = "PRODUCTION"
    _write_policy(path, value)
    assert "POLICY_RUNTIME_NOT_VALIDATION_ONLY" in check_policy(path)["reason_codes"]


def test_prohibited_activation_enforcement(tmp_path):
    path = tmp_path / "policy.md"
    value = _policy()
    value["activation_state"] = "ACTIVE"
    _write_policy(path, value)
    assert "POLICY_ACTIVATION_NOT_PROHIBITED" in check_policy(path)["reason_codes"]


def test_repository_policies_do_not_invent_values():
    history = (POLICY_DIR / POLICY_FILES["history_policy_status"]).read_text(
        encoding="utf-8"
    )
    marker = history.split("<!-- LENS_POLICY_JSON_V1 -->", 1)[1]
    policy = json.loads(marker.split("```json", 1)[1].split("```", 1)[0])
    for lens in policy["lens_policies"]:
        assert lens["minimum_observations"] is None
        assert lens["minimum_date_range"] is None
        assert lens["measurement_frequency"] is None
        assert lens["evidence_reference"] is None


def test_no_sys_dependency():
    checker = Path(
        "src/spine/jobs/equities/check_lens_contract_readiness_v1.py"
    ).read_text(encoding="utf-8")
    assert "spine.sys" not in checker
    assert "src.spine.sys" not in checker


def test_readiness_exposes_all_policy_statuses():
    results = check_lens_contract_readiness()
    for result in results:
        assert result["history_policy_status"] == "POLICY_INCOMPLETE"
        assert result["missing_observation_policy_status"] == "POLICY_INCOMPLETE"
        assert result["universe_policy_status"] == "POLICY_INCOMPLETE"

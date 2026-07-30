import json
from pathlib import Path

from spine.jobs.equities.check_lens_contract_readiness_v1 import (
    EVIDENCE_DIR,
    EVIDENCE_FILES,
    check_evidence,
    check_lens_contract_readiness,
)


def _write_evidence(path: Path, value: dict):
    path.write_text(
        "# Evidence\n\n<!-- LENS_EVIDENCE_JSON_V1 -->\n```json\n"
        + json.dumps(value, sort_keys=True)
        + "\n```\n",
        encoding="utf-8",
    )


def _evidence():
    return {
        "evidence_id": "TEST_EVIDENCE_V1",
        "evidence_version": "1.0.0",
        "evidence_status": "READY",
        "runtime_state": "VALIDATION_ONLY",
        "activation_state": "PROHIBITED",
    }


def test_missing_evidence_fails_closed(tmp_path):
    assert check_evidence(tmp_path / "missing.md") == {
        "status": "EVIDENCE_INCOMPLETE",
        "reason_codes": ["EVIDENCE_MISSING"],
    }


def test_malformed_evidence_fails_closed(tmp_path):
    path = tmp_path / "malformed.md"
    path.write_text("# no evidence block", encoding="utf-8")
    assert check_evidence(path)["reason_codes"] == ["EVIDENCE_MALFORMED"]


def test_evidence_output_is_deterministic():
    assert check_lens_contract_readiness() == check_lens_contract_readiness()


def test_repository_bindings_do_not_invent_values():
    for filename in EVIDENCE_FILES.values():
        text = (EVIDENCE_DIR / filename).read_text(encoding="utf-8")
        block = text.split("<!-- LENS_EVIDENCE_JSON_V1 -->", 1)[1]
        evidence = json.loads(block.split("```json", 1)[1].split("```", 1)[0])
        assert evidence["evidence_status"] == "EVIDENCE_INCOMPLETE"
        assert evidence.get("thresholds", []) == []
        assert evidence.get("symbols", []) == []
        assert evidence.get("handling_rules", []) == []


def test_no_sys_dependency():
    checker = Path(
        "src/spine/jobs/equities/check_lens_contract_readiness_v1.py"
    ).read_text(encoding="utf-8")
    assert "spine.sys" not in checker
    assert "src.spine.sys" not in checker


def test_runtime_and_activation_remain_fail_closed(tmp_path):
    path = tmp_path / "evidence.md"
    value = _evidence()
    value["runtime_state"] = "PRODUCTION"
    value["activation_state"] = "ACTIVE"
    _write_evidence(path, value)
    result = check_evidence(path)
    assert "EVIDENCE_RUNTIME_NOT_VALIDATION_ONLY" in result["reason_codes"]
    assert "EVIDENCE_ACTIVATION_NOT_PROHIBITED" in result["reason_codes"]


def test_readiness_exposes_all_evidence_statuses():
    for result in check_lens_contract_readiness():
        assert result["history_evidence_status"] == "EVIDENCE_INCOMPLETE"
        assert result["missing_observation_evidence_status"] == "EVIDENCE_INCOMPLETE"
        assert result["universe_evidence_status"] == "EVIDENCE_INCOMPLETE"

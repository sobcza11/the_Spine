import shutil
from pathlib import Path

from spine.jobs.equities.audit_lens_governance_evidence_v1 import (
    audit_lens_governance_evidence,
)
from spine.jobs.equities.check_lens_contract_readiness_v1 import (
    CONTRACT_DIR,
    EVIDENCE_DIR,
    EVIDENCE_FILES,
    POLICY_DIR,
)


def test_current_audit_covers_all_three_lenses():
    results = audit_lens_governance_evidence()
    assert [result["lens_id"] for result in results] == [
        "MARKET_BREADTH",
        "VOLATILITY_STRUCTURE",
        "LIQUIDITY_FLOWS",
    ]
    for result in results:
        assert result["contract_status"] == "CONTRACT_INCOMPLETE"
        assert result["policy_status"] == "POLICY_INCOMPLETE"
        assert result["evidence_status"] == "EVIDENCE_INCOMPLETE"
        assert result["overall_status"] == "BLOCKED"


def test_missing_evidence_fails_closed(tmp_path):
    results = audit_lens_governance_evidence(evidence_dir=tmp_path)
    assert all(result["overall_status"] == "BLOCKED" for result in results)
    assert all(
        any("EVIDENCE_MISSING" in item for item in result["missing_requirements"])
        for result in results
    )


def test_malformed_evidence_fails_closed(tmp_path):
    for filename in EVIDENCE_FILES.values():
        (tmp_path / filename).write_text("# malformed", encoding="utf-8")
    results = audit_lens_governance_evidence(evidence_dir=tmp_path)
    assert all(result["evidence_status"] == "EVIDENCE_INCOMPLETE" for result in results)
    assert all(
        any("EVIDENCE_MALFORMED" in item for item in result["missing_requirements"])
        for result in results
    )


def test_audit_output_is_deterministic():
    assert audit_lens_governance_evidence() == audit_lens_governance_evidence()


def test_validation_only_and_activation_prohibited():
    for result in audit_lens_governance_evidence():
        assert result["runtime_state"] == "VALIDATION_ONLY"
        assert result["activation_state"] == "PROHIBITED"


def test_no_sys_imports():
    source = Path(
        "src/spine/jobs/equities/audit_lens_governance_evidence_v1.py"
    ).read_text(encoding="utf-8")
    assert "spine.sys" not in source
    assert "src.spine.sys" not in source

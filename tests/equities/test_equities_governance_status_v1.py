from pathlib import Path

from spine.jobs.equities.report_equities_governance_status_v1 import (
    report_equities_governance_status,
)


def test_current_release_status_is_blocked():
    result = report_equities_governance_status()
    assert result["equities_status"] == "IN_PROGRESS"
    assert result["qqq_status"] == "SERVING_COMPLETE_LENS_BLOCKED"
    assert result["lens_status"] == "BLOCKED"
    assert result["blocking_items"]


def test_output_is_deterministic():
    assert report_equities_governance_status() == report_equities_governance_status()


def test_activation_remains_prohibited():
    result = report_equities_governance_status()
    assert result["runtime_state"] == "VALIDATION_ONLY"
    assert result["activation_state"] == "PROHIBITED"


def test_missing_audit_fails_closed():
    def unavailable():
        raise ValueError("missing")

    result = report_equities_governance_status(unavailable)
    assert result["lens_status"] == "BLOCKED"
    assert result["blocking_items"] == [
        "LENS_GOVERNANCE_AUDIT_UNAVAILABLE",
        "LENS_GOVERNANCE_SCOPE_INCOMPLETE",
    ]


def test_no_sys_dependency():
    source = Path(
        "src/spine/jobs/equities/report_equities_governance_status_v1.py"
    ).read_text(encoding="utf-8")
    assert "spine.sys" not in source
    assert "src.spine.sys" not in source

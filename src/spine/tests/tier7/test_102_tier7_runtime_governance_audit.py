from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\tier7_runtime_governance_audit.json"
)

def test_tier7_runtime_governance_audit():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "tier7-runtime-governance-audit"
    assert d["runtime_governance_audit_enabled"] is True
    assert d["governance_check_count"] >= 5

    assert d["write_boundary_contract"]["llm_writeback_allowed"] is False
    assert d["write_boundary_contract"]["autonomous_execution_allowed"] is False
    assert d["write_boundary_contract"]["mutation_requires_authorization"] is True

    assert d["escalation_contract"]["missing_file_escalates"] is True
    assert d["escalation_contract"]["executive_review_supported"] is True

    assert d["governance"]["runtime_governance_audit_governed"] is True
    assert d["governance"]["fail_closed_default"] is True

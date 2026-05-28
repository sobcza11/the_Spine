from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase3\institutional_cognition_maturity_audit.json"
)

def test_institutional_cognition_maturity_audit():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "institutional-cognition-maturity-audit"
    assert d["maturity_audit_enabled"] is True
    assert d["maturity_domain_count"] >= 8

    assert d["composite_maturity_score"] >= 8.5
    assert d["maturity_status"] == "INSTITUTIONAL_PLATFORM_EMERGING"

    assert d["audit_contract"]["architecture_scored"] is True
    assert d["audit_contract"]["predictive_validation_scored"] is True
    assert d["audit_contract"]["executive_usefulness_scored"] is True

    assert d["governance"]["maturity_audit_governed"] is True
    assert d["governance"]["score_is_advisory"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\constitutional\confidence_legitimacy_audit.json"
)

def test_confidence_legitimacy_audit():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "confidence-legitimacy-audit"
    assert d["confidence_legitimacy_audit_enabled"] is True
    assert d["confidence_legitimacy_check_count"] >= 7

    assert "historical_calibration_check" in d["confidence_legitimacy_checks"]
    assert "contradiction_penalty_check" in d["confidence_legitimacy_checks"]

    assert d["audit_contract"]["confidence_requires_calibration"] is True
    assert d["audit_contract"]["unearned_confidence_flagged"] is True

    assert d["governance"]["unearned_confidence_blocked"] is True
    assert d["governance"]["confidence_is_not_authority"] is True

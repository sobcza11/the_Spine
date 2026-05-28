from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase4\institutional_failure_audit_layer.json"
)

def test_institutional_failure_audit_layer():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "institutional-failure-audit-layer"
    assert d["institutional_failure_audit_enabled"] is True
    assert d["failure_audit_domain_count"] >= 7

    assert "forecast_failure_detection" in d["failure_audit_domains"]
    assert "signal_failure_detection" in d["failure_audit_domains"]
    assert "escalation_failure_detection" in d["failure_audit_domains"]

    assert d["failure_audit_contract"]["root_cause_analysis_required"] is True
    assert d["governance"]["failure_suppression_forbidden"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

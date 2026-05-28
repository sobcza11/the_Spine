from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\constitutional\institutional_proof_dashboard.json"
)

def test_institutional_proof_dashboard():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "institutional-proof-dashboard"
    assert d["institutional_proof_dashboard_enabled"] is True
    assert d["proof_panel_count"] >= 7

    assert "truth_governance_panel" in d["proof_panels"]
    assert "constitutional_violation_panel" in d["proof_panels"]

    assert d["dashboard_contract"]["constitutional_visibility_required"] is True
    assert d["dashboard_contract"]["executive_dashboard_required"] is True

    assert d["governance"]["hidden_constitutional_failures_forbidden"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

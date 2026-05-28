from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase6\institutional_cognition_continuity_audit.json"
)

def test_institutional_cognition_continuity_audit():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "institutional-cognition-continuity-audit"
    assert d["cognition_continuity_audit_enabled"] is True
    assert d["continuity_audit_domain_count"] >= 7

    assert "identity_continuity" in d["continuity_audit_domains"]
    assert "belief_state_continuity" in d["continuity_audit_domains"]

    assert d["continuity_contract"]["identity_continuity_required"] is True
    assert d["continuity_contract"]["macro_memory_continuity_required"] is True

    assert d["governance"]["continuity_breaks_visible"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

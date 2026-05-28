from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\constitutional\executive_comprehension_audit.json"
)

def test_executive_comprehension_audit():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "executive-comprehension-audit"
    assert d["executive_comprehension_audit_enabled"] is True
    assert d["comprehension_check_count"] >= 7

    assert "executive_summary_clarity" in d["comprehension_checks"]
    assert "actionability_boundary_clarity" in d["comprehension_checks"]

    assert d["comprehension_contract"]["executive_clarity_required"] is True
    assert d["comprehension_contract"]["plain_language_required"] is True

    assert d["governance"]["opaque_output_blocked"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

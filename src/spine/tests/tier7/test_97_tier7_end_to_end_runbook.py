from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\tier7_end_to_end_runbook.json"
)

def test_tier7_end_to_end_runbook():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "tier7-end-to-end-runbook"
    assert d["runbook_enabled"] is True
    assert d["runbook_step_count"] >= 5

    assert d["runbook_contract"]["one_command_sequence_documented"] is True
    assert d["runbook_contract"]["integration_test_included"] is True
    assert d["runbook_contract"]["dashboard_render_included"] is True

    assert d["governance"]["runbook_governance_enabled"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\executive_situational_awareness_theater.json"
)

def test_executive_situational_awareness_theater():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "executive-situational-awareness-theater"
    assert d["situational_awareness_theater_enabled"] is True
    assert d["theater_panel_count"] > 0

    assert d["theater_contract"]["executive_visibility_required"] is True
    assert d["theater_contract"]["runtime_state_visible"] is True
    assert d["theater_contract"]["decision_support_only"] is True

    assert d["governance"]["situational_awareness_governed"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
    assert d["governance"]["autonomous_execution_allowed"] is False

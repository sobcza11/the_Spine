from pathlib import Path
import json

P = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\agents\executive_escalation_agent.json")

def test_executive_escalation_agent():
    assert P.exists()
    d = json.loads(P.read_text(encoding="utf-8"))
    assert d["agent_name"] == "executive_escalation_agent"
    assert "priority" in d["task"]
    assert len(d["synthesis"]) > 0
    assert d["governance"]["llm_writeback_allowed"] is False

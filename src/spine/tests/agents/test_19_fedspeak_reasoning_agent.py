from pathlib import Path
import json

P = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\agents\fedspeak_reasoning_agent.json")

def test_fedspeak_reasoning_agent():
    assert P.exists()
    d = json.loads(P.read_text(encoding="utf-8"))
    assert d["agent_name"] == "fedspeak_reasoning_agent"
    assert "central_bank" in d["task"]
    assert len(d["synthesis"]) > 0
    assert d["governance"]["read_only"] is True

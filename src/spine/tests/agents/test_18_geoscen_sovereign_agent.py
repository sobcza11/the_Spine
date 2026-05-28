from pathlib import Path
import json

P = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\agents\geoscen_sovereign_agent.json")

def test_geoscen_sovereign_agent():
    assert P.exists()
    d = json.loads(P.read_text(encoding="utf-8"))
    assert d["agent_name"] == "geoscen_sovereign_agent"
    assert "sovereign" in d["task"]
    assert len(d["citations"]) > 0
    assert d["governance"]["citation_required"] is True

from pathlib import Path
import json

P = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\agents\agent_memory_router.json")

def test_agent_memory_router():
    assert P.exists()
    d = json.loads(P.read_text(encoding="utf-8"))
    assert "rbl_agent" in d["routes"]
    assert "geoscen_agent" in d["routes"]
    assert d["governance"]["citation_required"] is True

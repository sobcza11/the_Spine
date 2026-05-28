from pathlib import Path
import json

P = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\agents\historical_memory_agent.json")

def test_historical_memory_agent():
    assert P.exists()
    d = json.loads(P.read_text(encoding="utf-8"))
    assert d["agent_name"] == "historical_memory_agent"
    assert "historical" in d["task"]
    assert len(d["synthesis"]) > 0
    assert d["governance"]["the_spine_mutation_allowed"] is False

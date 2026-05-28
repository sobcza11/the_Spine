from pathlib import Path
import json

P = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier6\cross_agent_consensus_engine.json")

def test_cross_agent_consensus_engine():
    assert P.exists()
    d = json.loads(P.read_text(encoding="utf-8"))
    assert d["module"] == "cross-agent-consensus-engine"
    assert d["consensus_enabled"] is True
    assert d["agent_count"] > 0
    assert abs(d["total_weight"] - 1.0) < 0.001
    assert d["governance"]["disagreement_preserved"] is True

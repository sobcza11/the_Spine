from pathlib import Path
import json

P = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier6\institutional_reasoning_graph.json")

def test_institutional_reasoning_graph():
    assert P.exists()
    d = json.loads(P.read_text(encoding="utf-8"))
    assert d["module"] == "institutional-reasoning-graph"
    assert d["reasoning_graph_enabled"] is True
    assert d["node_count"] > 0
    assert d["edge_count"] > 0
    assert d["governance"]["graph_is_decision_support"] is True

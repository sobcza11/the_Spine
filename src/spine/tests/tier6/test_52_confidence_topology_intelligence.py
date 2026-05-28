from pathlib import Path
import json

P = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier6\confidence_topology_intelligence.json")

def test_confidence_topology_intelligence():
    assert P.exists()
    d = json.loads(P.read_text(encoding="utf-8"))
    assert d["module"] == "confidence-topology-intelligence"
    assert d["confidence_topology_enabled"] is True
    assert d["node_count"] > 0
    assert d["governance"]["confidence_is_not_certainty"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

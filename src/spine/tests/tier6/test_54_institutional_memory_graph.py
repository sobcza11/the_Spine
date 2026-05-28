from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier6\institutional_memory_graph.json"
)

def test_institutional_memory_graph():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "institutional-memory-graph"

    assert d["institutional_memory_graph_enabled"] is True

    assert d["memory_node_count"] > 0

    assert d["governance"]["memory_graph_read_only"] is True

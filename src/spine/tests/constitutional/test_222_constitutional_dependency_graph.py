from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\constitutional\constitutional_dependency_graph.json"
)

def test_constitutional_dependency_graph():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "constitutional-dependency-graph"
    assert d["constitutional_dependency_graph_enabled"] is True
    assert d["dependency_edge_count"] >= 7

    assert [
        "failure_admission_protocol",
        "post_mortem_doctrine_engine"
    ] in d["dependency_edges"]

    assert d["graph_contract"]["cross_system_mapping_required"] is True
    assert d["governance"]["dependency_drift_detection_required"] is True

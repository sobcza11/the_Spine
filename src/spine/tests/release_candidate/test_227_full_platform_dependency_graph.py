from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\release_candidate\full_platform_dependency_graph.json"
)

def test_full_platform_dependency_graph():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "full-platform-dependency-graph"
    assert d["full_platform_dependency_graph_enabled"] is True
    assert d["dependency_edge_count"] >= 6

    assert [
        "phase6_cognitive_sovereignty",
        "constitutional_proof_layer"
    ] in d["dependency_graph"]

    assert d["graph_contract"]["release_candidate_dependency_required"] is True
    assert d["governance"]["dependency_visibility_required"] is True

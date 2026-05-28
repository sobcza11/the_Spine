from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\global_macro_knowledge_graph.json"
)

def test_global_macro_knowledge_graph():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "global-macro-knowledge-graph"
    assert d["knowledge_graph_enabled"] is True
    assert d["graph_domain_count"] > 0

    assert d["graph_contract"]["entity_lineage_required"] is True
    assert d["graph_contract"]["relationship_provenance_required"] is True
    assert d["graph_contract"]["cross_domain_links_supported"] is True

    assert d["governance"]["knowledge_graph_governed"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
    assert d["governance"]["unverified_edges_allowed"] is False

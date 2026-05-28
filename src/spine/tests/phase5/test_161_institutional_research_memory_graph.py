from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase5\institutional_research_memory_graph.json"
)

def test_institutional_research_memory_graph():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "institutional-research-memory-graph"
    assert d["institutional_research_memory_graph_enabled"] is True
    assert d["memory_graph_node_count"] >= 8

    assert "hypothesis_nodes" in d["memory_graph_nodes"]
    assert "failed_experiment_nodes" in d["memory_graph_nodes"]
    assert "causal_claim_nodes" in d["memory_graph_nodes"]

    assert d["memory_graph_contract"]["research_lineage_required"] is True
    assert d["memory_graph_contract"]["failed_experiments_preserved"] is True

    assert d["governance"]["memory_write_requires_review"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

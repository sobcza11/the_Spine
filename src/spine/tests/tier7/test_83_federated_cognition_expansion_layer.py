from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\federated_cognition_expansion_layer.json"
)

def test_federated_cognition_expansion_layer():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "federated-cognition-expansion-layer"
    assert d["federated_expansion_enabled"] is True
    assert d["federation_domain_count"] > 0

    assert d["federation_contract"]["federated_nodes_governed"] is True
    assert d["federation_contract"]["node_provenance_required"] is True
    assert d["federation_contract"]["cross_node_conflict_visible"] is True

    assert d["governance"]["federated_cognition_governed"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
    assert d["governance"]["mutation_requires_authorization"] is True

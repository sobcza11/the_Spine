from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\constitutional\institutional_belief_registry.json"
)

def test_institutional_belief_registry():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "institutional-belief-registry"
    assert d["institutional_belief_registry_enabled"] is True
    assert d["belief_registry_field_count"] >= 8

    assert "belief_statement" in d["belief_registry_fields"]
    assert "contradicting_evidence" in d["belief_registry_fields"]

    assert d["registry_contract"]["confidence_required"] is True
    assert d["governance"]["unregistered_beliefs_blocked"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

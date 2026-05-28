from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase6\persistent_institutional_identity_layer.json"
)

def test_persistent_institutional_identity_layer():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "persistent-institutional-identity-layer"
    assert d["persistent_identity_enabled"] is True
    assert d["identity_continuity_domain_count"] >= 7

    assert "cognition_doctrine" in d["identity_continuity_domains"]
    assert "belief_state_history" in d["identity_continuity_domains"]

    assert d["identity_contract"]["long_term_identity_required"] is True
    assert d["identity_contract"]["memory_lineage_required"] is True

    assert d["governance"]["identity_mutation_requires_review"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

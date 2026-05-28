from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\human_ai_executive_collaboration_layer.json"
)

def test_human_ai_executive_collaboration_layer():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "human-ai-executive-collaboration-layer"
    assert d["collaboration_layer_enabled"] is True
    assert d["collaboration_domain_count"] > 0

    assert d["collaboration_contract"]["human_authority_required"] is True
    assert d["collaboration_contract"]["ai_assistance_read_only"] is True
    assert d["collaboration_contract"]["human_override_supported"] is True

    assert d["governance"]["collaboration_governance_enabled"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
    assert d["governance"]["autonomous_execution_allowed"] is False

from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\constitutional\human_authority_boundary_map.json"
)

def test_human_authority_boundary_map():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "human-authority-boundary-map"
    assert d["human_authority_boundary_map_enabled"] is True
    assert d["human_authority_domain_count"] >= 7

    assert "final_decision_authority" in d["human_authority_domains"]
    assert "override_authority" in d["human_authority_domains"]

    assert d["authority_contract"]["human_final_authority_required"] is True
    assert d["authority_contract"]["deployment_promotion_requires_human"] is True

    assert d["governance"]["ai_authority_escalation_blocked"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

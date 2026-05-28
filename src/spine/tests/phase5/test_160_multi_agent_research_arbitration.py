from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase5\multi_agent_research_arbitration.json"
)

def test_multi_agent_research_arbitration():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "multi-agent-research-arbitration"
    assert d["multi_agent_research_arbitration_enabled"] is True
    assert d["research_agent_role_count"] >= 7

    assert "baseline_skeptic_agent" in d["research_agent_roles"]
    assert "causal_validation_agent" in d["research_agent_roles"]

    assert d["arbitration_contract"]["competing_agent_review_required"] is True
    assert d["arbitration_contract"]["skeptic_agent_required"] is True

    assert d["governance"]["agent_consensus_not_sufficient"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

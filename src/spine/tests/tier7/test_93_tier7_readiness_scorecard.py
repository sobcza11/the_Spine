from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\tier7_readiness_scorecard.json"
)

def test_tier7_readiness_scorecard():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "tier7-readiness-scorecard"
    assert d["readiness_scorecard_enabled"] is True
    assert d["readiness_domain_count"] == 5

    assert d["composite_score"] >= 9.0
    assert d["readiness_status"] == "TIER_7_INTEGRATION_READY"

    assert d["scorecard_contract"]["governance_scored"] is True
    assert d["scorecard_contract"]["runtime_scored"] is True
    assert d["scorecard_contract"]["deployment_scored"] is True

    assert d["governance"]["scorecard_governance_enabled"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

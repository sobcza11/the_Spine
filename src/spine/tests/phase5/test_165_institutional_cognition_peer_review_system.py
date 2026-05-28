from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase5\institutional_cognition_peer_review_system.json"
)

def test_institutional_cognition_peer_review_system():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "institutional-cognition-peer-review-system"
    assert d["institutional_cognition_peer_review_enabled"] is True
    assert d["peer_review_check_count"] >= 7

    assert "hypothesis_review" in d["peer_review_checks"]
    assert "causal_claim_review" in d["peer_review_checks"]
    assert "promotion_approval_review" in d["peer_review_checks"]

    assert d["peer_review_contract"]["peer_review_required"] is True
    assert d["peer_review_contract"]["promotion_approval_required"] is True

    assert d["governance"]["unreviewed_research_promotion_forbidden"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

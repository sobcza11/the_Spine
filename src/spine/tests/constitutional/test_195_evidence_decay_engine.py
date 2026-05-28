from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\constitutional\evidence_decay_engine.json"
)

def test_evidence_decay_engine():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "evidence-decay-engine"
    assert d["evidence_decay_enabled"] is True
    assert d["decay_rule_count"] >= 7

    assert "expired_evidence_loses_claim_authority" in d["decay_rules"]
    assert "belief_support_requires_periodic_review" in d["decay_rules"]

    assert d["decay_contract"]["stale_evidence_degraded"] is True
    assert d["decay_contract"]["expired_evidence_loses_authority"] is True

    assert d["governance"]["stale_truth_blocked"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

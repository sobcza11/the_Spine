from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\constitutional\overconfidence_suppression_engine.json"
)

def test_overconfidence_suppression_engine():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "overconfidence-suppression-engine"
    assert d["overconfidence_suppression_enabled"] is True
    assert d["suppression_trigger_count"] >= 7

    assert "weak_evidence_high_confidence" in d["suppression_triggers"]
    assert "unsupported_causal_claim" in d["suppression_triggers"]

    assert d["suppression_contract"]["confidence_penalty_required"] is True
    assert d["suppression_contract"]["contradiction_penalty_required"] is True

    assert d["governance"]["unearned_confidence_blocked"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

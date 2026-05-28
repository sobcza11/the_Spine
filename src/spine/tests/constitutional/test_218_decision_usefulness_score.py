from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\constitutional\decision_usefulness_score.json"
)

def test_decision_usefulness_score():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "decision-usefulness-score"
    assert d["decision_usefulness_scoring_enabled"] is True
    assert d["usefulness_dimension_count"] >= 7

    assert "baseline_workflow_improvement" in d["usefulness_dimensions"]
    assert "executive_actionability_clarity" in d["usefulness_dimensions"]

    assert d["usefulness_contract"]["baseline_comparison_required"] is True
    assert d["usefulness_contract"]["usefulness_score_required"] is True

    assert d["governance"]["usefulness_claims_require_evidence"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

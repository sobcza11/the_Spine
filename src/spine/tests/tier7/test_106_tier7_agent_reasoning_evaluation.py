from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\tier7_agent_reasoning_evaluation.json"
)

def test_tier7_agent_reasoning_evaluation():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "tier7-agent-reasoning-evaluation"
    assert d["agent_reasoning_evaluation_enabled"] is True
    assert d["agent_evaluation_metric_count"] >= 7

    assert "hallucination_rate" in d["agent_evaluation_metrics"]
    assert "reasoning_drift_detection" in d["agent_evaluation_metrics"]
    assert "fallback_quality" in d["agent_evaluation_metrics"]

    assert d["evaluation_contract"]["hallucination_tracking_required"] is True
    assert d["evaluation_contract"]["source_grounding_required"] is True

    assert d["minimum_quality_targets"]["hallucination_rate_max"] <= 0.05

    assert d["governance"]["agent_evaluation_governed"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

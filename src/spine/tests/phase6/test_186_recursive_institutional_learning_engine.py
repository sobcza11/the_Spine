from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase6\recursive_institutional_learning_engine.json"
)

def test_recursive_institutional_learning_engine():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "recursive-institutional-learning-engine"
    assert d["recursive_learning_enabled"] is True
    assert d["learning_domain_count"] >= 7

    assert "forecast_failure_learning" in d["learning_domains"]
    assert "cross_cycle_adaptation_learning" in d["learning_domains"]

    assert d["learning_contract"]["failure_learning_required"] is True
    assert d["governance"]["untracked_learning_forbidden"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

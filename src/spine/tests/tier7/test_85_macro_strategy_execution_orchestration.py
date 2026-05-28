from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\macro_strategy_execution_orchestration.json"
)

def test_macro_strategy_execution_orchestration():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "macro-strategy-execution-orchestration"
    assert d["strategy_orchestration_enabled"] is True
    assert d["strategy_orchestration_domain_count"] > 0

    assert d["strategy_orchestration_contract"]["execution_is_not_autonomous"] is True
    assert d["strategy_orchestration_contract"]["human_approval_required"] is True
    assert d["strategy_orchestration_contract"]["governance_review_required"] is True

    assert d["governance"]["strategy_orchestration_governed"] is True
    assert d["governance"]["autonomous_execution_allowed"] is False
    assert d["governance"]["llm_writeback_allowed"] is False

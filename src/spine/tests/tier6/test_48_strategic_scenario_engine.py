from pathlib import Path
import json

P = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier6\strategic_scenario_engine.json")

def test_strategic_scenario_engine():
    assert P.exists()
    d = json.loads(P.read_text(encoding="utf-8"))
    assert d["module"] == "strategic-scenario-engine"
    assert d["scenario_engine_enabled"] is True
    assert d["scenario_count"] > 0
    assert d["governance"]["probabilistic_decision_support"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

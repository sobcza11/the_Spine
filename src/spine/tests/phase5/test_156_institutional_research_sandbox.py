from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase5\institutional_research_sandbox.json"
)

def test_institutional_research_sandbox():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "institutional-research-sandbox"
    assert d["institutional_research_sandbox_enabled"] is True
    assert d["sandbox_control_count"] >= 7

    assert "isolated_research_runtime" in d["sandbox_controls"]
    assert "no_production_writeback" in d["sandbox_controls"]

    assert d["sandbox_contract"]["production_writeback_forbidden"] is True
    assert d["sandbox_contract"]["promotion_requires_human_approval"] is True

    assert d["governance"]["research_sandbox_governed"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

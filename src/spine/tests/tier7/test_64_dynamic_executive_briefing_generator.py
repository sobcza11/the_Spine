from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\dynamic_executive_briefing_generator.json"
)

def test_dynamic_executive_briefing_generator():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "dynamic-executive-briefing-generator"
    assert d["briefing_generator_enabled"] is True
    assert d["briefing_section_count"] > 0

    assert d["briefing_contract"]["source_traceability_required"] is True
    assert d["briefing_contract"]["contradictions_must_survive"] is True
    assert d["briefing_contract"]["decision_support_only"] is True

    assert d["governance"]["briefing_governance_enabled"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
    assert d["governance"]["uncited_synthesis_allowed"] is False

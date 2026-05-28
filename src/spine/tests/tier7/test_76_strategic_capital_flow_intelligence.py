from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\strategic_capital_flow_intelligence.json"
)

def test_strategic_capital_flow_intelligence():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "strategic-capital-flow-intelligence"
    assert d["capital_flow_intelligence_enabled"] is True
    assert d["capital_flow_domain_count"] > 0

    assert d["capital_flow_contract"]["cross_border_pressure_visible"] is True
    assert d["capital_flow_contract"]["safe_haven_rotation_supported"] is True
    assert d["capital_flow_contract"]["sovereign_capital_pressure_supported"] is True

    assert d["governance"]["capital_flow_intelligence_governed"] is True
    assert d["governance"]["deterministic_inputs_authoritative"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

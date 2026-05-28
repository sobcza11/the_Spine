from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\institutional_cognition_operating_system.json"
)

def test_institutional_cognition_operating_system():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "institutional-cognition-operating-system"
    assert d["institutional_cognition_os_enabled"] is True
    assert d["operating_system_component_count"] > 0

    assert d["operating_system_contract"]["tier7_integration_complete"] is True
    assert d["operating_system_contract"]["deterministic_measurements_authoritative"] is True
    assert d["operating_system_contract"]["ai_interpretation_read_only"] is True

    assert d["governance"]["institutional_os_governed"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
    assert d["governance"]["mutation_requires_authorization"] is True

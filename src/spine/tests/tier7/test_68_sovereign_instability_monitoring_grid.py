from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\sovereign_instability_monitoring_grid.json"
)

def test_sovereign_instability_monitoring_grid():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "sovereign-instability-monitoring-grid"
    assert d["sovereign_monitoring_enabled"] is True
    assert d["sovereign_domain_count"] > 0

    assert d["monitoring_contract"]["sovereign_pressure_visible"] is True
    assert d["monitoring_contract"]["regional_contagion_supported"] is True
    assert d["monitoring_contract"]["executive_escalation_supported"] is True

    assert d["governance"]["sovereign_monitoring_governed"] is True
    assert d["governance"]["deterministic_inputs_authoritative"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

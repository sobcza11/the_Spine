from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\constitutional\political_pressure_firewall.json"
)

def test_political_pressure_firewall():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "political-pressure-firewall"
    assert d["political_pressure_firewall_enabled"] is True
    assert d["pressure_scenario_count"] >= 7

    assert "electoral_cycle_pressure" in d["pressure_scenarios"]
    assert "regulatory_influence_pressure" in d["pressure_scenarios"]

    assert d["firewall_contract"]["evidence_supremacy_required"] is True
    assert d["firewall_contract"]["independent_validation_required"] is True

    assert d["governance"]["evidence_overrides_politics"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

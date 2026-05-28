from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase6\macro_civilization_scale_scenario_systems.json"
)

def test_macro_civilization_scale_scenario_systems():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "macro-civilization-scale-scenario-systems"
    assert d["civilization_scale_scenarios_enabled"] is True
    assert d["scenario_domain_count"] >= 7

    assert "reserve_currency_transition" in d["scenario_domains"]
    assert "sovereign_debt_supercycle" in d["scenario_domains"]

    assert d["scenario_contract"]["scenario_not_prediction"] is True
    assert d["governance"]["false_certainty_blocked"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

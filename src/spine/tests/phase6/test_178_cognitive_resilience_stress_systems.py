from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase6\cognitive_resilience_stress_systems.json"
)

def test_cognitive_resilience_stress_systems():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "cognitive-resilience-stress-systems"
    assert d["cognitive_resilience_enabled"] is True
    assert d["resilience_stress_scenario_count"] >= 7

    assert "liquidity_shock" in d["resilience_stress_scenarios"]
    assert "extreme_uncertainty_regime" in d["resilience_stress_scenarios"]

    assert d["resilience_contract"]["graceful_degradation_required"] is True
    assert d["resilience_contract"]["fallback_modes_required"] is True

    assert d["governance"]["panic_mode_execution_forbidden"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

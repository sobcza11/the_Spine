from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase4\cross_cycle_robustness_testing.json"
)

def test_cross_cycle_robustness_testing():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "cross-cycle-robustness-testing"
    assert d["cross_cycle_robustness_testing_enabled"] is True
    assert d["robustness_cycle_count"] >= 8

    assert "inflation_cycle" in d["robustness_cycles"]
    assert "liquidity_shortage_cycle" in d["robustness_cycles"]
    assert "sovereign_stress_cycle" in d["robustness_cycles"]

    assert d["robustness_contract"]["multi_cycle_testing_required"] is True
    assert d["robustness_contract"]["signal_stability_required"] is True

    assert d["governance"]["single_cycle_overfit_visible"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

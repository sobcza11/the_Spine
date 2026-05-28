from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase6\institutional_strategic_adaptation_engine.json"
)

def test_institutional_strategic_adaptation_engine():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "institutional-strategic-adaptation-engine"
    assert d["institutional_strategic_adaptation_enabled"] is True
    assert d["adaptation_driver_count"] >= 7

    assert "historical_failure_learning" in d["adaptation_drivers"]
    assert "structural_break_detection" in d["adaptation_drivers"]

    assert d["adaptation_contract"]["adaptation_requires_evidence"] is True
    assert d["adaptation_contract"]["belief_update_traceability_required"] is True

    assert d["governance"]["uncontrolled_doctrine_drift_forbidden"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase3\frozen_signal_definitions.json"
)

def test_frozen_signal_definitions():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "frozen-signal-definitions"
    assert d["signal_freeze_enabled"] is True
    assert d["signal_definition_count"] >= 5

    assert len(d["formula_hash"]) == 64
    assert d["freeze_contract"]["post_outcome_formula_change_forbidden"] is True
    assert d["governance"]["silent_regime_fitting_blocked"] is True

from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\tier7_deterministic_signal_expansion.json"
)

def test_tier7_deterministic_signal_expansion():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "tier7-deterministic-signal-expansion"
    assert d["deterministic_signal_expansion_enabled"] is True
    assert d["signal_upgrade_count"] >= 7

    assert "liquidity" in d["signal_upgrades"]
    assert "inflation" in d["signal_upgrades"]
    assert "sovereign" in d["signal_upgrades"]

    assert d["signal_contract"]["deterministic_measurements_authoritative"] is True
    assert d["signal_contract"]["placeholder_cognition_to_be_replaced"] is True

    assert d["governance"]["signal_expansion_governed"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

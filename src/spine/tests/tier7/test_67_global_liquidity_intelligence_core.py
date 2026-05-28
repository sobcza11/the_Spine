from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\global_liquidity_intelligence_core.json"
)

def test_global_liquidity_intelligence_core():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "global-liquidity-intelligence-core"
    assert d["liquidity_intelligence_enabled"] is True
    assert d["liquidity_domain_count"] > 0

    assert d["liquidity_contract"]["deterministic_liquidity_inputs_required"] is True
    assert d["liquidity_contract"]["funding_stress_detection_supported"] is True
    assert d["liquidity_contract"]["executive_escalation_supported"] is True

    assert d["governance"]["liquidity_intelligence_governed"] is True
    assert d["governance"]["deterministic_inputs_authoritative"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase5\autonomous_historical_analog_mining.json"
)

def test_autonomous_historical_analog_mining():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "autonomous-historical-analog-mining"
    assert d["autonomous_historical_analog_mining_enabled"] is True
    assert d["analog_discovery_domain_count"] >= 7

    assert "liquidity_crisis_analogs" in d["analog_discovery_domains"]
    assert "regime_transition_analogs" in d["analog_discovery_domains"]

    assert d["analog_contract"]["cross_cycle_comparison_required"] is True
    assert d["analog_contract"]["false_analog_review_required"] is True

    assert d["governance"]["unsupported_historical_claims_forbidden"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

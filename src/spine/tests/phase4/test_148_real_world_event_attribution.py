from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase4\real_world_event_attribution.json"
)

def test_real_world_event_attribution():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "real-world-event-attribution"
    assert d["real_world_event_attribution_enabled"] is True
    assert d["attribution_domain_count"] >= 7

    assert "liquidity_events" in d["attribution_domains"]
    assert "sovereign_events" in d["attribution_domains"]
    assert "cross_asset_fractures" in d["attribution_domains"]

    assert d["attribution_contract"]["causal_traceability_required"] is True
    assert d["governance"]["unsupported_causality_forbidden"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase5\cross_asset_anomaly_discovery.json"
)

def test_cross_asset_anomaly_discovery():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "cross-asset-anomaly-discovery"
    assert d["cross_asset_anomaly_discovery_enabled"] is True
    assert d["anomaly_class_count"] >= 7

    assert "rates_equity_divergence" in d["anomaly_classes"]
    assert "cross_market_correlation_breakdown" in d["anomaly_classes"]

    assert d["anomaly_contract"]["novel_pattern_tracking_required"] is True
    assert d["governance"]["false_positive_tracking_required"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

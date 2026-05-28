from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase5\regime_clustering_intelligence.json"
)

def test_regime_clustering_intelligence():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "regime-clustering-intelligence"
    assert d["regime_clustering_intelligence_enabled"] is True
    assert d["clustering_domain_count"] >= 7

    assert "inflation_regimes" in d["clustering_domains"]
    assert "cross_asset_fracture_regimes" in d["clustering_domains"]

    assert d["clustering_contract"]["unsupervised_regime_detection_required"] is True
    assert d["governance"]["cluster_reclassification_visible"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

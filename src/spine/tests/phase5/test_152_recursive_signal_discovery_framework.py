from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase5\recursive_signal_discovery_framework.json"
)

def test_recursive_signal_discovery_framework():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "recursive-signal-discovery-framework"
    assert d["recursive_signal_discovery_enabled"] is True
    assert d["discovery_method_count"] >= 7

    assert "cross_asset_divergence_scanning" in d["discovery_methods"]
    assert "narrative_shift_detection" in d["discovery_methods"]

    assert d["discovery_contract"]["candidate_signal_tracking_required"] is True
    assert d["governance"]["signal_promotion_requires_review"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

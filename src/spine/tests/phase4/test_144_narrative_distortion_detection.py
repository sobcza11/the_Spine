from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase4\narrative_distortion_detection.json"
)

def test_narrative_distortion_detection():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "narrative-distortion-detection"
    assert d["narrative_distortion_detection_enabled"] is True
    assert d["distortion_channel_count"] >= 7

    assert "source_concentration_bias" in d["distortion_channels"]
    assert "unsupported_causal_claims" in d["distortion_channels"]
    assert "propaganda_contamination_risk" in d["distortion_channels"]

    assert d["distortion_contract"]["unsupported_causality_detection_required"] is True
    assert d["governance"]["neutrality_required"] is True
    assert d["governance"]["uncited_synthesis_allowed"] is False

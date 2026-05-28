from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\constitutional\uncertainty_preservation_layer.json"
)

def test_uncertainty_preservation_layer():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "uncertainty-preservation-layer"
    assert d["uncertainty_preservation_enabled"] is True
    assert d["uncertainty_type_count"] >= 7

    assert "causal_uncertainty" in d["uncertainty_types"]
    assert "forecast_uncertainty" in d["uncertainty_types"]

    assert d["uncertainty_contract"]["false_certainty_forbidden"] is True
    assert d["governance"]["certainty_compression_blocked"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\constitutional\fragility_disclosure_layer.json"
)

def test_fragility_disclosure_layer():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "fragility-disclosure-layer"
    assert d["fragility_disclosure_enabled"] is True
    assert d["fragility_domain_count"] >= 7

    assert "thin_source_coverage" in d["fragility_domains"]
    assert "unresolved_contradictions" in d["fragility_domains"]

    assert d["fragility_contract"]["fragility_visibility_required"] is True
    assert d["fragility_contract"]["weak_signal_disclosure_required"] is True

    assert d["governance"]["weakness_hiding_forbidden"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

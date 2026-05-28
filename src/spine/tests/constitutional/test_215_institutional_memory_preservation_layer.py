from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\constitutional\institutional_memory_preservation_layer.json"
)

def test_institutional_memory_preservation_layer():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "institutional-memory-preservation-layer"
    assert d["institutional_memory_preservation_enabled"] is True
    assert d["memory_domain_count"] >= 7
    assert len(d["memory_schema_hash"]) == 64

    assert "forecast_history" in d["memory_domains"]
    assert "constitutional_history" in d["memory_domains"]

    assert d["memory_contract"]["historical_memory_required"] is True
    assert d["memory_contract"]["failure_memory_required"] is True

    assert d["governance"]["historical_erasure_forbidden"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

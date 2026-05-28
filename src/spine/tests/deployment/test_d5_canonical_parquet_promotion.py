from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\deployment\canonical_parquet_promotion.json"
)

def test_canonical_parquet_promotion():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "canonical-parquet-promotion"
    assert d["canonical_parquet_promotion_enabled"] is True
    assert d["promotion_requirement_count"] >= 5

    assert "snapshot_creation_required" in d["promotion_requirements"]

    assert d["promotion_contract"]["immutable_snapshot_required"] is True
    assert d["governance"]["replayability_required"] is True

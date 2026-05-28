from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\release_candidate\real_vs_scaffold_classifier.json"
)

def test_real_vs_scaffold_classifier():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "real-vs-scaffold-classifier"
    assert d["real_vs_scaffold_classifier_enabled"] is True
    assert d["classification_bucket_count"] >= 2

    assert "production_real" in d["classification_buckets"]
    assert "scaffold_ready" in d["classification_buckets"]

    assert d["classifier_contract"]["maturity_boundary_required"] is True
    assert d["governance"]["false_production_claims_forbidden"] is True

from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase4\prediction_accountability_ledger.json"
)

def test_prediction_accountability_ledger():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "prediction-accountability-ledger"
    assert d["prediction_accountability_ledger_enabled"] is True
    assert d["ledger_field_count"] >= 10
    assert len(d["ledger_schema_hash"]) == 64

    assert "prediction_timestamp" in d["ledger_fields"]
    assert "confidence_score" in d["ledger_fields"]
    assert "realized_outcome" in d["ledger_fields"]

    assert d["ledger_contract"]["source_artifacts_required"] is True
    assert d["ledger_contract"]["post_mortem_required"] is True

    assert d["governance"]["immutability_required"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

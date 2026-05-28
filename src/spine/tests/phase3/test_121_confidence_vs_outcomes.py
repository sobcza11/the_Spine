from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase3\confidence_vs_outcomes.json"
)

def test_confidence_vs_outcomes():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "confidence-vs-outcomes"
    assert d["confidence_outcome_calibration_enabled"] is True
    assert d["confidence_bucket_count"] == 5

    assert d["calibration_contract"]["realized_outcome_required"] is True
    assert d["calibration_contract"]["calibration_error_required"] is True
    assert d["calibration_contract"]["overconfidence_penalty_required"] is True

    assert d["governance"]["confidence_is_not_certainty"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

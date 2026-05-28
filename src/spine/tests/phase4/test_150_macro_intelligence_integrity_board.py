from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase4\macro_intelligence_integrity_board.json"
)

def test_macro_intelligence_integrity_board():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "macro-intelligence-integrity-board"
    assert d["macro_intelligence_integrity_board_enabled"] is True
    assert d["integrity_board_function_count"] >= 8

    assert "forecast_review" in d["integrity_board_functions"]
    assert "failure_audit_review" in d["integrity_board_functions"]
    assert "signal_survivorship_review" in d["integrity_board_functions"]

    assert d["integrity_contract"]["independent_review_required"] is True
    assert d["integrity_contract"]["failure_review_required"] is True

    assert d["governance"]["independent_oversight_required"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

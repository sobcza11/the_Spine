from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase4\macro_signal_survivorship_analysis.json"
)

def test_macro_signal_survivorship_analysis():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "macro-signal-survivorship-analysis"
    assert d["signal_survivorship_analysis_enabled"] is True
    assert d["survivorship_test_count"] >= 7

    assert "out_of_sample_signal_survival" in d["survivorship_tests"]
    assert "weak_signal_removal_review" in d["survivorship_tests"]

    assert d["survivorship_contract"]["weak_signal_removal_required"] is True
    assert d["governance"]["survivorship_bias_visible"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

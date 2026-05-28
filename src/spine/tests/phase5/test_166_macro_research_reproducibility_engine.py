from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase5\macro_research_reproducibility_engine.json"
)

def test_macro_research_reproducibility_engine():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "macro-research-reproducibility-engine"
    assert d["macro_research_reproducibility_enabled"] is True
    assert d["reproducibility_control_count"] >= 7

    assert "frozen_input_datasets" in d["reproducibility_controls"]
    assert "deterministic_rebuilds" in d["reproducibility_controls"]

    assert d["reproducibility_contract"]["deterministic_rebuilds_required"] is True
    assert d["reproducibility_contract"]["immutable_outcomes_required"] is True

    assert d["governance"]["mutable_historical_results_forbidden"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

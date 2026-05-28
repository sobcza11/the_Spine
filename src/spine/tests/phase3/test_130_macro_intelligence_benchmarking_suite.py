from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase3\macro_intelligence_benchmarking_suite.json"
)

def test_macro_intelligence_benchmarking_suite():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "macro-intelligence-benchmarking-suite"
    assert d["benchmarking_suite_enabled"] is True
    assert d["benchmark_dimension_count"] >= 8

    assert "regime_detection_accuracy" in d["benchmark_dimensions"]
    assert "baseline_outperformance" in d["benchmark_dimensions"]
    assert "operator_usefulness_score" in d["benchmark_dimensions"]

    assert d["benchmark_contract"]["baseline_outperformance_required"] is True
    assert d["benchmark_contract"]["operator_usefulness_required"] is True

    assert d["governance"]["claims_require_benchmark_evidence"] is True

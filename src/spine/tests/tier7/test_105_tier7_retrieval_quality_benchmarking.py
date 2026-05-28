from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\tier7_retrieval_quality_benchmarking.json"
)

def test_tier7_retrieval_quality_benchmarking():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "tier7-retrieval-quality-benchmarking"
    assert d["retrieval_quality_benchmarking_enabled"] is True
    assert d["rag_benchmark_metric_count"] >= 7
    assert d["benchmark_source_family_count"] >= 5

    assert "citation_accuracy" in d["rag_benchmark_metrics"]
    assert "hallucination_rate" in d["rag_benchmark_metrics"]
    assert "contradiction_preservation" in d["rag_benchmark_metrics"]

    assert d["benchmark_contract"]["citation_accuracy_required"] is True
    assert d["benchmark_contract"]["hallucination_tracking_required"] is True

    assert d["minimum_quality_targets"]["citation_accuracy_min"] >= 0.90

    assert d["governance"]["retrieval_benchmarking_governed"] is True
    assert d["governance"]["uncited_synthesis_allowed"] is False

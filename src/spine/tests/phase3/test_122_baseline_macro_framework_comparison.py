from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase3\baseline_macro_framework_comparison.json"
)

def test_baseline_macro_framework_comparison():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "baseline-macro-framework-comparison"
    assert d["baseline_comparison_enabled"] is True
    assert d["baseline_count"] >= 5

    assert "naive_no_change_baseline" in d["baselines"]
    assert "yield_curve_only_baseline" in d["baselines"]

    assert d["comparison_contract"]["outperformance_required_for_claims"] is True
    assert d["governance"]["complexity_must_earn_its_place"] is True

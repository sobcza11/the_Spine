from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "phase3"
OUT_PATH = OUT_DIR / "macro_intelligence_benchmarking_suite.json"


BENCHMARK_DIMENSIONS = [
    "regime_detection_accuracy",
    "lead_time_before_event",
    "false_positive_rate",
    "false_negative_rate",
    "confidence_calibration_error",
    "baseline_outperformance",
    "operator_usefulness_score",
    "contradiction_signal_value",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "macro-intelligence-benchmarking-suite",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "benchmarking_suite_enabled": True,

        "benchmark_dimensions": BENCHMARK_DIMENSIONS,
        "benchmark_dimension_count": len(BENCHMARK_DIMENSIONS),

        "benchmark_objective": (
            "Benchmark macro intelligence quality against baseline frameworks using "
            "regime accuracy, event lead time, false positives, false negatives, confidence "
            "calibration, baseline outperformance, operator usefulness, and contradiction value."
        ),

        "benchmark_contract": {
            "regime_accuracy_required": True,
            "false_positive_tracking_required": True,
            "false_negative_tracking_required": True,
            "baseline_outperformance_required": True,
            "operator_usefulness_required": True,
        },

        "governance": {
            "benchmarking_suite_governed": True,
            "claims_require_benchmark_evidence": True,
            "llm_writeback_allowed": False,
            "audit_trail_required": True,
        },
    }

    OUT_PATH.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )

    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()

from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "phase4"
OUT_PATH = OUT_DIR / "macro_signal_survivorship_analysis.json"


SURVIVORSHIP_TESTS = [
    "signal_persistence_across_windows",
    "out_of_sample_signal_survival",
    "decade_level_signal_survival",
    "false_positive_adjusted_survival",
    "baseline_relative_survival",
    "regime_specific_failure_detection",
    "weak_signal_removal_review",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "macro-signal-survivorship-analysis",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "signal_survivorship_analysis_enabled": True,

        "survivorship_tests": SURVIVORSHIP_TESTS,
        "survivorship_test_count": len(SURVIVORSHIP_TESTS),

        "survivorship_objective": (
            "Identify which macro signals survive across validation windows, regimes, "
            "decades, baselines, and false-positive adjustment, while removing weak or "
            "non-persistent signals."
        ),

        "survivorship_contract": {
            "out_of_sample_survival_required": True,
            "cross_window_survival_required": True,
            "baseline_relative_survival_required": True,
            "weak_signal_removal_required": True,
            "human_review_required": True,
        },

        "governance": {
            "signal_survivorship_governed": True,
            "survivorship_bias_visible": True,
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

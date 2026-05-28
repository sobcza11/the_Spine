from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "phase3"
OUT_PATH = OUT_DIR / "out_of_sample_degradation_tracking.json"


DEGRADATION_METRICS = [
    "train_test_accuracy_gap",
    "precision_decay",
    "recall_decay",
    "false_positive_increase",
    "confidence_calibration_decay",
    "signal_stability_decay",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "out-of-sample-degradation-tracking",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "out_of_sample_tracking_enabled": True,

        "degradation_metrics": DEGRADATION_METRICS,
        "degradation_metric_count": len(DEGRADATION_METRICS),

        "tracking_objective": (
            "Track whether macro signal performance decays outside calibration windows "
            "and whether confidence remains reliable out-of-sample."
        ),

        "tracking_contract": {
            "train_test_split_required": True,
            "degradation_metrics_required": True,
            "out_of_sample_results_required": True,
            "performance_decay_visible": True,
            "human_review_required": True,
        },

        "governance": {
            "out_of_sample_tracking_governed": True,
            "overfit_risk_visible": True,
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

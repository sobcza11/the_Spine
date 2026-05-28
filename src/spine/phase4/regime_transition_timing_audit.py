from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "phase4"
OUT_PATH = OUT_DIR / "regime_transition_timing_audit.json"


TIMING_AUDIT_METRICS = [
    "early_detection_days",
    "late_detection_days",
    "missed_transition_count",
    "premature_transition_count",
    "transition_confirmation_delay",
    "signal_lead_time",
    "operator_escalation_delay",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "regime-transition-timing-audit",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "regime_transition_timing_audit_enabled": True,

        "timing_audit_metrics": TIMING_AUDIT_METRICS,
        "timing_audit_metric_count": len(TIMING_AUDIT_METRICS),

        "timing_objective": (
            "Measure whether regime transitions are detected early, late, prematurely, "
            "or missed, including signal lead time and operator escalation delay."
        ),

        "timing_contract": {
            "transition_ground_truth_required": True,
            "early_late_measurement_required": True,
            "missed_transition_tracking_required": True,
            "premature_transition_tracking_required": True,
            "human_review_required": True,
        },

        "governance": {
            "timing_audit_governed": True,
            "late_detection_visible": True,
            "premature_detection_visible": True,
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

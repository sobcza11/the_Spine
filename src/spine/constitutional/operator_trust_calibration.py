from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "constitutional"
OUT_PATH = OUT_DIR / "operator_trust_calibration.json"


TRUST_CALIBRATION_DIMENSIONS = [
    "forecast_reliability",
    "confidence_legitimacy",
    "source_traceability",
    "contradiction_visibility",
    "failure_history_visibility",
    "operator_override_frequency",
    "decision_support_quality",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "operator-trust-calibration",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "operator_trust_calibration_enabled": True,

        "trust_calibration_dimensions": TRUST_CALIBRATION_DIMENSIONS,
        "trust_calibration_dimension_count": len(TRUST_CALIBRATION_DIMENSIONS),

        "calibration_objective": (
            "Measure where operators should trust institutional cognition based on reliability, "
            "confidence legitimacy, traceability, contradictions, failure history, overrides, and decision usefulness."
        ),

        "calibration_contract": {
            "operator_trust_measurement_required": True,
            "trust_boundaries_required": True,
            "failure_history_visible": True,
            "override_tracking_required": True,
            "human_review_required": True,
        },

        "governance": {
            "operator_trust_calibration_governed": True,
            "blind_trust_blocked": True,
            "trust_must_be_earned": True,
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

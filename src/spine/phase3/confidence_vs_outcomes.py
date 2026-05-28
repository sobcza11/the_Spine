from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "phase3"
OUT_PATH = OUT_DIR / "confidence_vs_outcomes.json"


CONFIDENCE_BUCKETS = [
    "0.00_0.20",
    "0.20_0.40",
    "0.40_0.60",
    "0.60_0.80",
    "0.80_1.00",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "confidence-vs-outcomes",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "confidence_outcome_calibration_enabled": True,

        "confidence_buckets": CONFIDENCE_BUCKETS,
        "confidence_bucket_count": len(CONFIDENCE_BUCKETS),

        "calibration_objective": (
            "Measure whether confidence scores correspond to realized outcomes, "
            "not just model certainty or narrative strength."
        ),

        "calibration_contract": {
            "confidence_bucket_scoring_required": True,
            "realized_outcome_required": True,
            "calibration_error_required": True,
            "overconfidence_penalty_required": True,
            "human_review_required": True,
        },

        "governance": {
            "confidence_outcome_calibration_governed": True,
            "confidence_is_not_certainty": True,
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

from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "constitutional"
OUT_PATH = OUT_DIR / "decision_usefulness_score.json"


USEFULNESS_DIMENSIONS = [
    "baseline_workflow_improvement",
    "risk_awareness_improvement",
    "time_to_understanding_reduction",
    "false_positive_reduction",
    "decision_context_quality",
    "operator_confidence_alignment",
    "executive_actionability_clarity",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "decision-usefulness-score",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "decision_usefulness_scoring_enabled": True,

        "usefulness_dimensions": USEFULNESS_DIMENSIONS,
        "usefulness_dimension_count": len(USEFULNESS_DIMENSIONS),

        "usefulness_objective": (
            "Measure whether institutional cognition improves decision support compared with baseline workflows "
            "through better risk awareness, clarity, speed, context, and reduced false positives."
        ),

        "usefulness_contract": {
            "baseline_comparison_required": True,
            "operator_feedback_required": True,
            "decision_context_required": True,
            "usefulness_score_required": True,
            "human_review_required": True,
        },

        "governance": {
            "decision_usefulness_governed": True,
            "usefulness_claims_require_evidence": True,
            "decorative_intelligence_penalized": True,
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

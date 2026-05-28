from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "constitutional"
OUT_PATH = OUT_DIR / "executive_comprehension_audit.json"


COMPREHENSION_CHECKS = [
    "executive_summary_clarity",
    "signal_explanation_clarity",
    "confidence_explanation_clarity",
    "contradiction_visibility_clarity",
    "actionability_boundary_clarity",
    "source_traceability_clarity",
    "risk_language_clarity",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "executive-comprehension-audit",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "executive_comprehension_audit_enabled": True,

        "comprehension_checks": COMPREHENSION_CHECKS,
        "comprehension_check_count": len(COMPREHENSION_CHECKS),

        "comprehension_objective": (
            "Test whether executive users can understand summaries, signals, confidence, contradictions, "
            "actionability boundaries, source traceability, and risk language."
        ),

        "comprehension_contract": {
            "executive_clarity_required": True,
            "plain_language_required": True,
            "confidence_explanation_required": True,
            "actionability_boundary_required": True,
            "human_review_required": True,
        },

        "governance": {
            "executive_comprehension_governed": True,
            "opaque_output_blocked": True,
            "decision_boundary_must_be_visible": True,
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

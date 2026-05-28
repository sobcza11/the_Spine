from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "constitutional"
OUT_PATH = OUT_DIR / "confidence_legitimacy_audit.json"


CONFIDENCE_LEGITIMACY_CHECKS = [
    "evidence_support_check",
    "source_quality_check",
    "historical_calibration_check",
    "contradiction_penalty_check",
    "uncertainty_visibility_check",
    "forecast_error_history_check",
    "operator_review_check",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "confidence-legitimacy-audit",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "confidence_legitimacy_audit_enabled": True,

        "confidence_legitimacy_checks": CONFIDENCE_LEGITIMACY_CHECKS,
        "confidence_legitimacy_check_count": len(CONFIDENCE_LEGITIMACY_CHECKS),

        "audit_objective": (
            "Test whether confidence is earned through evidence support, source quality, "
            "historical calibration, contradiction penalties, uncertainty visibility, forecast history, and review."
        ),

        "audit_contract": {
            "confidence_requires_evidence": True,
            "confidence_requires_calibration": True,
            "contradiction_penalty_required": True,
            "unearned_confidence_flagged": True,
            "human_review_required": True,
        },

        "governance": {
            "confidence_legitimacy_governed": True,
            "unearned_confidence_blocked": True,
            "confidence_is_not_authority": True,
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

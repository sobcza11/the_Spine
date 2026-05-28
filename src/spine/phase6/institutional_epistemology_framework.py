from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "phase6"
OUT_PATH = OUT_DIR / "institutional_epistemology_framework.json"


EPISTEMIC_PRINCIPLES = [
    "evidence_before_conviction",
    "contradictions_must_persist",
    "confidence_requires_calibration",
    "causality_over_correlation",
    "uncertainty_must_be_visible",
    "historical_failures_preserved",
    "beliefs_require_traceability",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "institutional-epistemology-framework",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "institutional_epistemology_enabled": True,

        "epistemic_principles": EPISTEMIC_PRINCIPLES,
        "epistemic_principle_count": len(EPISTEMIC_PRINCIPLES),

        "epistemology_objective": (
            "Formalize institutional cognition principles governing evidence, contradiction, "
            "confidence, causality, uncertainty, historical failures, and belief traceability."
        ),

        "epistemology_contract": {
            "evidence_required": True,
            "uncertainty_visibility_required": True,
            "contradiction_preservation_required": True,
            "causal_validation_required": True,
            "human_review_required": True,
        },

        "governance": {
            "epistemology_governed": True,
            "unsupported_conviction_forbidden": True,
            "narrative_capture_blocked": True,
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

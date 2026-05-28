from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "constitutional"
OUT_PATH = OUT_DIR / "contradiction_preservation_doctrine.json"


CONTRADICTION_CLASSES = [
    "signal_contradiction",
    "source_contradiction",
    "model_contradiction",
    "narrative_contradiction",
    "forecast_contradiction",
    "belief_contradiction",
    "operator_contradiction",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "contradiction-preservation-doctrine",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "contradiction_preservation_enabled": True,

        "contradiction_classes": CONTRADICTION_CLASSES,
        "contradiction_class_count": len(CONTRADICTION_CLASSES),

        "doctrine_objective": (
            "Keep unresolved institutional conflict visible across signals, sources, models, "
            "narratives, forecasts, beliefs, and operator judgments."
        ),

        "contradiction_contract": {
            "contradictions_must_remain_visible": True,
            "premature_resolution_forbidden": True,
            "confidence_penalty_required": True,
            "executive_visibility_required": True,
            "human_review_required": True,
        },

        "governance": {
            "contradiction_preservation_governed": True,
            "false_coherence_blocked": True,
            "narrative_smoothing_forbidden": True,
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

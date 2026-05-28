from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "constitutional"
OUT_PATH = OUT_DIR / "overconfidence_suppression_engine.json"


SUPPRESSION_TRIGGERS = [
    "weak_evidence_high_confidence",
    "single_source_overreliance",
    "unresolved_contradiction",
    "poor_historical_calibration",
    "model_instability",
    "forecast_error_cluster",
    "unsupported_causal_claim",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "overconfidence-suppression-engine",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "overconfidence_suppression_enabled": True,

        "suppression_triggers": SUPPRESSION_TRIGGERS,
        "suppression_trigger_count": len(SUPPRESSION_TRIGGERS),

        "suppression_objective": (
            "Penalize unjustified conviction when confidence is high but evidence, sources, "
            "calibration, stability, causality, or contradiction handling is weak."
        ),

        "suppression_contract": {
            "confidence_penalty_required": True,
            "weak_evidence_detection_required": True,
            "single_source_penalty_required": True,
            "contradiction_penalty_required": True,
            "human_review_required": True,
        },

        "governance": {
            "overconfidence_suppression_governed": True,
            "unearned_confidence_blocked": True,
            "hallucinated_conviction_blocked": True,
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

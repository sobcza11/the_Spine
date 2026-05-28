from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

TIER7_DIR = ROOT / "tier7"
OUT_PATH = TIER7_DIR / "tier7_confidence_score_calibration.json"


CONFIDENCE_COMPONENTS = {
    "source_quality": {
        "weight": 0.20,
        "description": "Trustworthiness and institutional quality of source inputs",
    },
    "data_freshness": {
        "weight": 0.16,
        "description": "Recency and timeliness of source data",
    },
    "signal_agreement": {
        "weight": 0.16,
        "description": "Degree of alignment across deterministic signals",
    },
    "historical_support": {
        "weight": 0.16,
        "description": "Support from comparable historical regimes",
    },
    "model_stability": {
        "weight": 0.12,
        "description": "Stability of output across repeated runs",
    },
    "provenance_completeness": {
        "weight": 0.12,
        "description": "Completeness of source lineage and citation trail",
    },
    "contradiction_penalty": {
        "weight": 0.08,
        "description": "Penalty for unresolved or severe contradictions",
    },
}


def main():
    TIER7_DIR.mkdir(parents=True, exist_ok=True)

    total_weight = round(
        sum(v["weight"] for v in CONFIDENCE_COMPONENTS.values()),
        4,
    )

    payload = {
        "system": "IsoVector",
        "module": "tier7-confidence-score-calibration",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "confidence_score_calibration_enabled": True,

        "confidence_components": CONFIDENCE_COMPONENTS,

        "confidence_component_count": len(CONFIDENCE_COMPONENTS),

        "total_component_weight": total_weight,

        "calibration_objective": (
            "Convert heuristic Tier 7 confidence into measured confidence using source "
            "quality, freshness, signal agreement, historical support, model stability, "
            "provenance completeness, and contradiction penalties."
        ),

        "confidence_contract": {
            "component_weights_required": True,
            "weights_sum_to_one": total_weight == 1.0,
            "confidence_is_measured": True,
            "confidence_is_not_certainty": True,
            "human_review_required": True,
        },

        "governance": {
            "confidence_calibration_governed": True,
            "confidence_is_advisory": True,
            "llm_writeback_allowed": False,
            "human_review_required": True,
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

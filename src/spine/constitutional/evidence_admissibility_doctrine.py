from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "constitutional"
OUT_PATH = OUT_DIR / "evidence_admissibility_doctrine.json"


ADMISSIBLE_EVIDENCE_CLASSES = [
    "audited_macro_data",
    "official_institutional_sources",
    "validated_internal_signals",
    "traceable_historical_outcomes",
    "operator_reviewed_claims",
    "peer_reviewed_research",
    "governed_runtime_artifacts",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "evidence-admissibility-doctrine",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "evidence_admissibility_enabled": True,

        "admissible_evidence_classes": ADMISSIBLE_EVIDENCE_CLASSES,
        "admissible_evidence_class_count": len(ADMISSIBLE_EVIDENCE_CLASSES),

        "doctrine_objective": (
            "Define what counts as constitutionally admissible institutional evidence "
            "before claims, forecasts, beliefs, or escalations can enter cognition."
        ),

        "admissibility_contract": {
            "evidence_required": True,
            "source_traceability_required": True,
            "unsupported_claims_forbidden": True,
            "narrative_only_evidence_insufficient": True,
            "human_review_required": True,
        },

        "governance": {
            "evidence_doctrine_governed": True,
            "inadmissible_evidence_blocked": True,
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

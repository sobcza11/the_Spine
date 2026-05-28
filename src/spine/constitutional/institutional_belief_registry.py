from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "constitutional"
OUT_PATH = OUT_DIR / "institutional_belief_registry.json"


BELIEF_REGISTRY_FIELDS = [
    "belief_id",
    "belief_statement",
    "belief_category",
    "supporting_evidence",
    "contradicting_evidence",
    "confidence_score",
    "last_reviewed_utc",
    "review_status",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "institutional-belief-registry",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "institutional_belief_registry_enabled": True,

        "belief_registry_fields": BELIEF_REGISTRY_FIELDS,
        "belief_registry_field_count": len(BELIEF_REGISTRY_FIELDS),

        "registry_objective": (
            "Track what the institution believes, why it believes it, what evidence supports "
            "or contradicts it, confidence level, review status, and ownership."
        ),

        "registry_contract": {
            "belief_statement_required": True,
            "supporting_evidence_required": True,
            "contradicting_evidence_required": True,
            "confidence_required": True,
            "human_review_required": True,
        },

        "governance": {
            "belief_registry_governed": True,
            "unregistered_beliefs_blocked": True,
            "belief_drift_visible": True,
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

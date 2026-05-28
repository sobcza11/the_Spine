from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "constitutional"
OUT_PATH = OUT_DIR / "claim_burden_of_proof_layer.json"


CLAIM_REQUIREMENTS = [
    "claim_statement",
    "evidence_class",
    "source_provenance",
    "confidence_score",
    "causal_status",
    "contradicting_evidence",
    "human_review_status",
    "expiration_or_refresh_date",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "claim-burden-of-proof-layer",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "claim_burden_of_proof_enabled": True,

        "claim_requirements": CLAIM_REQUIREMENTS,
        "claim_requirement_count": len(CLAIM_REQUIREMENTS),

        "proof_objective": (
            "Require every institutional claim to carry evidence, provenance, confidence, "
            "causal status, contradiction review, human review, and refresh discipline."
        ),

        "proof_contract": {
            "evidence_required_for_every_claim": True,
            "provenance_required_for_every_claim": True,
            "confidence_required_for_every_claim": True,
            "contradiction_review_required": True,
            "unsupported_claims_forbidden": True,
        },

        "governance": {
            "burden_of_proof_governed": True,
            "claim_without_evidence_blocked": True,
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

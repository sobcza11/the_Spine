from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "constitutional"
OUT_PATH = OUT_DIR / "source_integrity_constitution.json"


SOURCE_INTEGRITY_CHECKS = [
    "source_identity_verified",
    "source_freshness_checked",
    "source_bias_reviewed",
    "source_provenance_recorded",
    "source_authority_ranked",
    "source_contradictions_preserved",
    "source_reuse_audited",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "source-integrity-constitution",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "source_integrity_constitution_enabled": True,

        "source_integrity_checks": SOURCE_INTEGRITY_CHECKS,
        "source_integrity_check_count": len(SOURCE_INTEGRITY_CHECKS),

        "source_objective": (
            "Govern source reliability, freshness, provenance, authority, bias, contradiction, "
            "and reuse before evidence can influence institutional cognition."
        ),

        "source_contract": {
            "source_identity_required": True,
            "freshness_check_required": True,
            "provenance_required": True,
            "authority_ranking_required": True,
            "bias_review_required": True,
        },

        "governance": {
            "source_integrity_governed": True,
            "unverified_sources_blocked": True,
            "source_decay_visible": True,
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

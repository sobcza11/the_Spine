from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "constitutional"
OUT_PATH = OUT_DIR / "truth_hierarchy_engine.json"


TRUTH_HIERARCHY = {
    "tier_1": "audited_or_official_data",
    "tier_2": "validated_internal_measurements",
    "tier_3": "reproducible_historical_findings",
    "tier_4": "operator_reviewed_analysis",
    "tier_5": "model_generated_hypotheses",
    "tier_6": "external_narratives",
    "tier_7": "unverified_claims",
}


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "truth-hierarchy-engine",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "truth_hierarchy_enabled": True,

        "truth_hierarchy": TRUTH_HIERARCHY,
        "truth_hierarchy_level_count": len(TRUTH_HIERARCHY),

        "hierarchy_objective": (
            "Rank institutional knowledge inputs so audited data, validated signals, "
            "and reproducible findings outrank hypotheses, narratives, and unverified claims."
        ),

        "hierarchy_contract": {
            "truth_ranking_required": True,
            "official_data_priority_required": True,
            "unverified_claims_lowest_authority": True,
            "model_hypotheses_not_truth": True,
            "human_review_required": True,
        },

        "governance": {
            "truth_hierarchy_governed": True,
            "information_democracy_blocked": True,
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

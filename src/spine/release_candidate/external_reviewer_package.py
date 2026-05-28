from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "release_candidate"
OUT_PATH = OUT_DIR / "external_reviewer_package.json"


REVIEWER_ARTIFACTS = [
    "master_system_registry",
    "platform_dependency_graph",
    "readiness_scorecard",
    "constitutional_proof_summary",
    "validation_runner",
    "governance_overview",
    "real_vs_scaffold_classifier",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "external-reviewer-package",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "external_reviewer_package_enabled": True,

        "reviewer_artifacts": REVIEWER_ARTIFACTS,
        "reviewer_artifact_count": len(REVIEWER_ARTIFACTS),

        "package_objective": (
            "Create a concise evidence package for external reviewers showing "
            "architecture, governance, validation, constitutional controls, and maturity boundaries."
        ),

        "package_contract": {
            "executive_summary_required": True,
            "validation_visibility_required": True,
            "governance_visibility_required": True,
            "maturity_boundary_visibility_required": True,
            "human_review_required": True,
        },

        "governance": {
            "external_reviewer_package_governed": True,
            "misleading_capability_claims_forbidden": True,
            "validation_gap_visibility_required": True,
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

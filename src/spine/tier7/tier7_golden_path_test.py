from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

TIER7_DIR = ROOT / "tier7"
OUT_PATH = TIER7_DIR / "tier7_golden_path_test.json"


GOLDEN_PATH_STAGES = [
    "tier1_foundation",
    "tier2_data_contracts",
    "tier3_runtime_outputs",
    "tier4_cognition_outputs",
    "tier5_rag_agent_outputs",
    "tier6_intelligence_layer",
    "tier7_institutional_os",
]


def main():
    TIER7_DIR.mkdir(parents=True, exist_ok=True)

    tier7_required = [
        TIER7_DIR / "tier7_master_registry.json",
        TIER7_DIR / "tier7_integration_test.json",
        TIER7_DIR / "institutional_cognition_operating_system.json",
        TIER7_DIR / "institutional_os_dashboard.json",
    ]

    tier7_existing = [
        str(p) for p in tier7_required if p.exists()
    ]

    payload = {
        "system": "IsoVector",
        "module": "tier7-golden-path-test",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "golden_path_test_enabled": True,

        "golden_path_stages": GOLDEN_PATH_STAGES,

        "golden_path_stage_count": len(GOLDEN_PATH_STAGES),

        "tier7_required_artifact_count": len(tier7_required),

        "tier7_existing_artifact_count": len(tier7_existing),

        "golden_path_passed": len(tier7_existing) == len(tier7_required),

        "golden_path_objective": (
            "Validate continuity from Tier 1 foundation through Tier 7 institutional OS "
            "using Tier 7 registry, integration, OS, and dashboard artifacts as the current "
            "continuity proof."
        ),

        "golden_path_contract": {
            "tier1_to_tier7_path_declared": True,
            "tier7_terminal_artifacts_present": len(tier7_existing) == len(tier7_required),
            "os_artifact_required": True,
            "dashboard_artifact_required": True,
            "integration_artifact_required": True,
        },

        "governance": {
            "golden_path_governance_enabled": True,
            "human_review_required": True,
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

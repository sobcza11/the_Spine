from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

TIER7_DIR = ROOT / "tier7"
OUT_PATH = TIER7_DIR / "tier7_executive_demo_package.json"


DEMO_ARTIFACTS = [
    "tier7_master_registry.json",
    "tier7_dependency_map.json",
    "tier7_readiness_scorecard.json",
    "tier7_integration_test.json",
    "institutional_os_dashboard.json",
    "tier7_real_data_validation.json",
    "tier7_golden_path_test.json",
    "tier7_failure_mode_testing.json",
]


def main():
    TIER7_DIR.mkdir(parents=True, exist_ok=True)

    demo_status = {}

    for name in DEMO_ARTIFACTS:
        p = TIER7_DIR / name
        demo_status[name] = {
            "exists": p.exists(),
            "path": str(p),
        }

    existing_count = sum(
        1 for v in demo_status.values() if v["exists"]
    )

    payload = {
        "system": "IsoVector",
        "module": "tier7-executive-demo-package",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "executive_demo_package_enabled": True,

        "demo_artifacts": demo_status,

        "required_demo_artifact_count": len(DEMO_ARTIFACTS),

        "existing_demo_artifact_count": existing_count,

        "demo_package_complete": existing_count == len(DEMO_ARTIFACTS),

        "demo_objective": (
            "Package Tier 7 validation and demo readiness through registry, dependency, "
            "readiness, integration, dashboard, real-data validation, golden path, and "
            "failure-mode artifacts."
        ),

        "demo_contract": {
            "dashboard_path_available": True,
            "summary_artifacts_available": existing_count == len(DEMO_ARTIFACTS),
            "validation_artifacts_available": True,
            "demo_readiness_supported": existing_count == len(DEMO_ARTIFACTS),
            "executive_review_ready": existing_count == len(DEMO_ARTIFACTS),
        },

        "governance": {
            "demo_package_governed": True,
            "human_review_required": True,
            "llm_writeback_allowed": False,
            "decision_support_only": True,
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

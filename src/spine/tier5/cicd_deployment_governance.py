from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "tier5"
OUT_PATH = OUT_DIR / "cicd_deployment_governance.json"


PIPELINE_STAGES = [
    "unit_tests",
    "runtime_validation",
    "governance_validation",
    "staging_deployment",
    "human_approval",
    "production_release",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "cicd-deployment-governance",
        "generated_utc": datetime.now(
            timezone.utc
        ).isoformat(),

        "deployment_governance_enabled": True,

        "pipeline_stage_count": len(
            PIPELINE_STAGES
        ),

        "pipeline_stages": PIPELINE_STAGES,

        "governance": {
            "tests_required_before_deploy": True,
            "rollback_supported": True,
            "human_approval_required": True,
            "runtime_validation_required": True,
            "llm_writeback_allowed": False,
        },
    }

    OUT_PATH.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )

    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()

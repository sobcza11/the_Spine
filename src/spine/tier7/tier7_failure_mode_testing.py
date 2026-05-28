from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

TIER7_DIR = ROOT / "tier7"
OUT_PATH = TIER7_DIR / "tier7_failure_mode_testing.json"


FAILURE_MODES = [
    "missing_file",
    "stale_artifact",
    "empty_payload",
    "cache_rebuild_required",
    "missing_provenance",
    "failed_runtime_dependency",
    "dashboard_not_rendered",
]


def main():
    TIER7_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "tier7-failure-mode-testing",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "failure_mode_testing_enabled": True,

        "failure_modes": FAILURE_MODES,

        "failure_mode_count": len(FAILURE_MODES),

        "failure_mode_objective": (
            "Define Tier 7 fallback, cache, stale-data, missing-file, missing-provenance, "
            "dependency-failure, and dashboard-render failure behavior."
        ),

        "failure_contract": {
            "missing_file_behavior_defined": True,
            "stale_artifact_behavior_defined": True,
            "cache_rebuild_behavior_defined": True,
            "missing_provenance_behavior_defined": True,
            "dashboard_failure_behavior_defined": True,
        },

        "fallback_policy": {
            "missing_file": "fail_closed_and_report_missing_artifact",
            "stale_artifact": "mark_stale_and_require_rebuild",
            "empty_payload": "quarantine_artifact",
            "cache_rebuild_required": "rerun_module_sequence",
            "missing_provenance": "reject_cognition_output",
            "failed_runtime_dependency": "degrade_to_available_artifacts",
            "dashboard_not_rendered": "preserve_json_artifacts_and_fail_dashboard_only",
        },

        "governance": {
            "failure_testing_governed": True,
            "human_review_required": True,
            "llm_writeback_allowed": False,
            "fail_closed_default": True,
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

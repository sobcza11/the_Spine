from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "release_candidate"
OUT_PATH = OUT_DIR / "executive_release_dashboard.json"


DASHBOARD_PANELS = [
    "master_system_registry",
    "platform_dependency_graph",
    "readiness_scorecard",
    "validation_runner_status",
    "constitutional_proof_status",
    "truth_calibration_status",
    "real_world_validation_gap",
    "rc1_declaration_status",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "executive-release-dashboard",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "executive_release_dashboard_enabled": True,

        "dashboard_panels": DASHBOARD_PANELS,
        "dashboard_panel_count": len(DASHBOARD_PANELS),

        "dashboard_status": "RC1_EXECUTIVE_VIEW_READY",

        "dashboard_objective": (
            "Render one executive release-candidate view showing registry, dependencies, "
            "readiness, validation, constitutional proof, truth calibration, validation gaps, "
            "and RC1 status."
        ),

        "dashboard_contract": {
            "executive_view_required": True,
            "readiness_visible": True,
            "validation_gap_visible": True,
            "constitutional_status_visible": True,
            "rc1_status_visible": True,
        },

        "governance": {
            "executive_release_dashboard_governed": True,
            "release_candidate_not_marked_done": True,
            "executive_visibility_required": True,
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

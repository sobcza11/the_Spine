from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

DASHBOARD_DIR = ROOT / "dashboards"

OUT_DIR = ROOT / "deployment"
OUT_PATH = OUT_DIR / "executive_offline_render_package.json"


RENDER_ARTIFACTS = [
    "executive_html_dashboard",
    "executive_pdf_packet",
    "macro_regime_summary",
    "contradiction_heatmap",
    "geoscen_snapshot",
    "runtime_audit_summary",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "executive-offline-render-package",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "executive_offline_render_package_enabled": True,

        "dashboard_directory": str(DASHBOARD_DIR),

        "render_artifacts": RENDER_ARTIFACTS,
        "render_artifact_count": len(RENDER_ARTIFACTS),

        "render_objective": (
            "Generate governed offline executive cognition render packages "
            "for institutional review without live runtime dependencies."
        ),

        "render_contract": {
            "offline_html_render_required": True,
            "offline_pdf_render_required": True,
            "executive_summary_required": True,
            "audit_visibility_required": True,
            "human_review_required": True,
        },

        "governance": {
            "offline_render_governed": True,
            "ungoverned_external_rendering_forbidden": True,
            "offline_distribution_required": True,
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

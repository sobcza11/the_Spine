from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "runtime_living"
OUT_PATH = OUT_DIR / "live_cognition_dashboard.json"


DASHBOARDS = [
    "rbl_dashboard",
    "contradiction_dashboard",
    "geoscen_dashboard",
    "historical_dashboard",
    "runtime_health_dashboard",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "live-cognition-dashboard",
        "generated_utc": datetime.now(
            timezone.utc
        ).isoformat(),

        "persistent_live_cognition": True,

        "dashboard_count": len(DASHBOARDS),

        "dashboards": DASHBOARDS,

        "runtime_features": {
            "event_driven_updates": True,
            "persistent_runtime_memory": True,
            "cross_agent_rendering": True,
            "live_refresh_enabled": True,
        },

        "governance": {
            "dashboard_governance_enabled": True,
            "runtime_audit_required": True,
            "human_review_required": True,
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

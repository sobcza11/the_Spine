from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "runtime_living"
OUT_PATH = OUT_DIR / "runtime_dependency_graph.json"


DEPENDENCIES = [
    {
        "source": "rates_plane",
        "target": "contradiction_agent",
    },
    {
        "source": "fx_plane",
        "target": "rbl_agent",
    },
    {
        "source": "contradiction_agent",
        "target": "executive_escalation_agent",
    },
    {
        "source": "geoscen_agent",
        "target": "rbl_agent",
    },
    {
        "source": "historical_memory_agent",
        "target": "executive_dashboard",
    },
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "runtime-dependency-graph",
        "generated_utc": datetime.now(
            timezone.utc
        ).isoformat(),

        "dependency_tracking_enabled": True,

        "dependency_count": len(DEPENDENCIES),

        "dependencies": DEPENDENCIES,

        "governance": {
            "cross_system_update_tracking": True,
            "dependency_audit_required": True,
            "runtime_propagation_governed": True,
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

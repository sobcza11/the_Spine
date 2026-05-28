from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "tier5"
OUT_PATH = OUT_DIR / "dockerized_runtime_segments.json"


SEGMENTS = [
    "rbl_runtime",
    "geoscen_runtime",
    "runtime_event_bus",
    "dashboard_runtime",
    "agent_runtime",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "dockerized-runtime-segments",
        "generated_utc": datetime.now(
            timezone.utc
        ).isoformat(),

        "containerization_enabled": True,

        "segments": SEGMENTS,

        "docker_targets": {
            "docker_compose": True,
            "isolated_runtime_domains": True,
            "reproducible_builds": True,
        },

        "governance": {
            "runtime_isolation_enabled": True,
            "deployment_consistency_required": True,
            "container_audit_required": True,
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

from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "runtime_living"
OUT_PATH = OUT_DIR / "cognitive_refresh_schedule.json"


SCHEDULES = [
    {
        "system": "fx_plane",
        "refresh_minutes": 5,
    },
    {
        "system": "rates_plane",
        "refresh_minutes": 15,
    },
    {
        "system": "rbl_agent",
        "refresh_minutes": 30,
    },
    {
        "system": "geoscen_agent",
        "refresh_minutes": 60,
    },
    {
        "system": "historical_overlays",
        "refresh_minutes": 1440,
    },
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "cognitive-refresh-scheduler",
        "generated_utc": datetime.now(
            timezone.utc
        ).isoformat(),

        "refresh_orchestration_enabled": True,

        "schedules": SCHEDULES,

        "governance": {
            "controlled_refresh_cycles": True,
            "runtime_throttling_enabled": True,
            "stale_runtime_detection": True,
            "human_override_supported": True,
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

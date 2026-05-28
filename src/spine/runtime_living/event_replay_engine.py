from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "runtime_living"
OUT_PATH = OUT_DIR / "event_replay_log.json"

EVENT_BUS = ROOT / "runtime_living" / "runtime_event_bus.json"


def load_json(path: Path) -> dict:
    if not path.exists():
        return {}

    return json.loads(
        path.read_text(encoding="utf-8")
    )


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    bus = load_json(EVENT_BUS)

    replay_events = []

    for idx, event in enumerate(
        bus.get("events", [])
    ):
        replay_events.append({
            "replay_id": idx + 1,
            "event_type": event.get("event_type"),
            "source": event.get("source"),
            "target": event.get("target"),
            "severity": event.get("severity"),
            "replayed_utc": datetime.now(
                timezone.utc
            ).isoformat(),
        })

    payload = {
        "system": "IsoVector",
        "module": "event-replay-engine",
        "generated_utc": datetime.now(
            timezone.utc
        ).isoformat(),

        "replay_supported": True,

        "replay_event_count": len(replay_events),

        "replay_events": replay_events,

        "governance": {
            "runtime_reconstruction_enabled": True,
            "historical_state_recovery": True,
            "event_audit_required": True,
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

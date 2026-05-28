from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")
OUT_DIR = ROOT / "runtime_living"
OUT_PATH = OUT_DIR / "websocket_synchronization.json"


CHANNELS = [
    {
        "channel": "rbl_agent",
        "source": "langroid_rbl_agent_output.json",
        "enabled": True,
    },
    {
        "channel": "contradiction_agent",
        "source": "contradiction_reasoning_agent.json",
        "enabled": True,
    },
    {
        "channel": "geoscen_agent",
        "source": "geoscen_sovereign_agent.json",
        "enabled": True,
    },
    {
        "channel": "runtime_events",
        "source": "runtime_event_bus.json",
        "enabled": True,
    },
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "websocket-synchronization",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "status": "local_sync_skeleton",
        "channels": CHANNELS,
        "governance": {
            "local_only": True,
            "read_only_streaming": True,
            "state_mutation_requires_event": True,
            "audit_required": True,
        },
    }

    OUT_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()

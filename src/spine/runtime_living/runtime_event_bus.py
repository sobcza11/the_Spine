from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")
OUT_DIR = ROOT / "runtime_living"
OUT_PATH = OUT_DIR / "runtime_event_bus.json"


EVENTS = [
    {
        "event_type": "rbl_synthesis_updated",
        "source": "langroid_rbl_agent_output.json",
        "target": "executive_dashboard",
        "severity": "medium",
    },
    {
        "event_type": "contradiction_state_updated",
        "source": "contradiction_reasoning_agent.json",
        "target": "rbl_agent",
        "severity": "high",
    },
    {
        "event_type": "geoscen_sovereign_updated",
        "source": "geoscen_sovereign_agent.json",
        "target": "executive_escalation_agent",
        "severity": "medium",
    },
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "runtime-event-bus",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "event_count": len(EVENTS),
        "events": EVENTS,
        "governance": {
            "event_routing_enabled": True,
            "read_only_events": True,
            "mutation_requires_governance": True,
            "event_audit_required": True,
        },
    }

    OUT_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()

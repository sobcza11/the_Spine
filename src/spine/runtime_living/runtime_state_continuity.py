from pathlib import Path
from datetime import datetime, timezone
import json
import hashlib


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "runtime_living"
OUT_PATH = OUT_DIR / "runtime_state_continuity.json"

STATE_FILES = [
    ROOT / "agents" / "contradiction_reasoning_agent.json",
    ROOT / "agents" / "executive_escalation_agent.json",
    ROOT / "agents" / "geoscen_sovereign_agent.json",
    ROOT / "agents" / "historical_memory_agent.json",
    ROOT / "runtime_living" / "runtime_event_bus.json",
]


def file_hash(path: Path) -> str:
    if not path.exists():
        return "missing"

    return hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


def snapshot(path: Path) -> dict:
    return {
        "file": path.name,
        "path": str(path),
        "exists": path.exists(),
        "hash": file_hash(path),
    }


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "runtime-state-continuity",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "persistent_runtime_memory": True,

        "snapshots": [
            snapshot(p)
            for p in STATE_FILES
        ],

        "governance": {
            "runtime_state_persistence": True,
            "mutation_tracking_required": True,
            "reconstruction_supported": True,
            "read_only_inputs": True,
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

from pathlib import Path
from datetime import datetime, timezone
import json
import hashlib


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")
OUT_DIR = ROOT / "runtime_living"
OUT_PATH = OUT_DIR / "incremental_runtime_state.json"

WATCH_FILES = [
    ROOT / "agents" / "contradiction_reasoning_agent.json",
    ROOT / "agents" / "executive_escalation_agent.json",
    ROOT / "agents" / "geoscen_sovereign_agent.json",
    ROOT / "rbl_agent" / "langroid_rbl_agent_output.json",
]


def file_hash(path: Path) -> str:
    if not path.exists():
        return "missing"
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "incremental-refresh-runtime",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "refresh_mode": "partial_cognition_mutation",
        "watched_artifacts": [
            {
                "file": p.name,
                "path": str(p),
                "exists": p.exists(),
                "hash": file_hash(p),
            }
            for p in WATCH_FILES
        ],
        "governance": {
            "partial_refresh_only": True,
            "full_rebuild_required": False,
            "read_only_inputs": True,
            "mutation_audit_required": True,
            "llm_writeback_allowed": False,
        },
    }

    OUT_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()

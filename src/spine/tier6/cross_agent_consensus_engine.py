from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")
OUT_DIR = ROOT / "tier6"
OUT_PATH = OUT_DIR / "cross_agent_consensus_engine.json"

AGENTS = [
    {
        "agent": "rbl_agent",
        "weight": 0.25,
    },
    {
        "agent": "contradiction_agent",
        "weight": 0.25,
    },
    {
        "agent": "geoscen_sovereign_agent",
        "weight": 0.20,
    },
    {
        "agent": "fedspeak_reasoning_agent",
        "weight": 0.15,
    },
    {
        "agent": "historical_memory_agent",
        "weight": 0.15,
    },
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    total_weight = round(sum(x["weight"] for x in AGENTS), 4)

    payload = {
        "system": "OracleChambers",
        "module": "cross-agent-consensus-engine",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "consensus_enabled": True,
        "agents": AGENTS,
        "agent_count": len(AGENTS),
        "total_weight": total_weight,
        "consensus_state": "conditional_alignment",
        "governance": {
            "multi_agent_review_required": True,
            "disagreement_preserved": True,
            "human_review_required": True,
            "llm_writeback_allowed": False,
        },
    }

    OUT_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()

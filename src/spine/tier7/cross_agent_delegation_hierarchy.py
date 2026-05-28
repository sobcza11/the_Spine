from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "tier7"
OUT_PATH = OUT_DIR / "cross_agent_delegation_hierarchy.json"


AGENT_HIERARCHY = [
    "executive_routing_agent",
    "rbl_agent",
    "geoscen_agent",
    "fedspeak_agent",
    "contradiction_agent",
    "historical_memory_agent",
    "governance_review_agent",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "cross-agent-delegation-hierarchy",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "agent_delegation_enabled": True,

        "agent_hierarchy": AGENT_HIERARCHY,

        "agent_count": len(AGENT_HIERARCHY),

        "delegation_objective": (
            "Coordinate read-only institutional agents across executive routing, "
            "macro interpretation, sovereign cognition, FedSpeak, contradiction "
            "analysis, historical memory, and governance review."
        ),

        "delegation_contract": {
            "agents_read_only": True,
            "delegation_trace_required": True,
            "source_constrained_reasoning_required": True,
            "human_review_required": True,
            "agent_conflict_visible": True,
        },

        "governance": {
            "agent_delegation_governed": True,
            "llm_writeback_allowed": False,
            "autonomous_execution_allowed": False,
            "audit_trail_required": True,
        },
    }

    OUT_PATH.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )

    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()

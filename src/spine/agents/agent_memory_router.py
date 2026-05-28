from pathlib import Path
from datetime import datetime, timezone
import json

from spine.agents.base_agent_runtime import load_json, save_json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")
OUT = ROOT / "agents" / "agent_memory_router.json"
RAG = ROOT / "rag" / "rag_retrieval_results.json"


def main():
    rag = load_json(RAG)

    payload = {
        "system": "OracleChambers",
        "module": "agent-memory-router",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "routes": {
            "rbl_agent": rag.get("retrieval_sets", {}).get("rbl_contextual", []),
            "geoscen_agent": rag.get("retrieval_sets", {}).get("geoscen_sovereign", []),
            "hybrid_memory": rag.get("retrieval_sets", {}).get("hybrid_combined", []),
        },
        "governance": {
            "read_only": True,
            "citation_required": True,
            "llm_writeback_allowed": False,
        },
    }

    save_json(OUT, payload)
    print(f"Wrote -> {OUT}")


if __name__ == "__main__":
    main()

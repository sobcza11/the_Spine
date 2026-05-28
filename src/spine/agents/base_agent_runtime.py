from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")
OUT_DIR = ROOT / "agents"
RAG_RESULTS = ROOT / "rag" / "rag_retrieval_results.json"


def load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_rag_set(name: str) -> list[dict]:
    data = load_json(RAG_RESULTS)
    return data.get("retrieval_sets", {}).get(name, [])


def governed_agent_output(
    module: str,
    agent_name: str,
    task: str,
    synthesis: list[str],
    citations: list[dict],
) -> dict:
    return {
        "system": "OracleChambers",
        "module": module,
        "agent_name": agent_name,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "task": task,
        "synthesis": synthesis,
        "citations": citations,
        "governance": {
            "read_only": True,
            "llm_writeback_allowed": False,
            "the_spine_mutation_allowed": False,
            "citation_required": True,
            "human_review_required": True,
            "local_inference_allowed": True,
        },
    }


def main():
    payload = {
        "system": "OracleChambers",
        "module": "base-agent-runtime",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "runtime": {
            "local_llm": "ollama",
            "model": "llama3.1:8b",
            "rag_results": str(RAG_RESULTS),
            "cache_supported": True,
            "fallback_supported": True,
        },
        "governance": {
            "read_only": True,
            "llm_writeback_allowed": False,
            "the_spine_mutation_allowed": False,
            "citation_required": True,
        },
    }

    save_json(OUT_DIR / "base_agent_runtime.json", payload)
    print(f"Wrote -> {OUT_DIR / 'base_agent_runtime.json'}")


if __name__ == "__main__":
    main()

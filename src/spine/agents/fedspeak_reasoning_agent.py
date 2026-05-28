from pathlib import Path

from spine.agents.base_agent_runtime import (
    load_json,
    save_json,
    load_rag_set,
    governed_agent_output,
)


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")
OUT = ROOT / "agents" / "fedspeak_reasoning_agent.json"


def main():
    fedspeak = load_json(ROOT / "tier3" / "fedspeak_cognition.json")
    rag = load_json(ROOT / "tier3" / "controlled_rag_retrieval.json")
    citations = load_rag_set("hybrid_combined")[:3]

    signal_lines = []
    for item in fedspeak.get("signals", []):
        signal_lines.append(
            f"{item.get('signal')}: {item.get('state')} ({item.get('score')})"
        )

    synthesis = [
        "Fedspeak cognition is active as a governed semantic interpretation layer.",
        *signal_lines,
        "Policy-language interpretation should remain grounded to controlled RAG retrieval rules.",
        f"Retrieval scope count: {len(rag.get('retrieval_scope', []))}.",
    ]

    payload = governed_agent_output(
        module="fedspeak-reasoning-agent",
        agent_name="fedspeak_reasoning_agent",
        task="central_bank_interpretation",
        synthesis=synthesis,
        citations=citations,
    )

    save_json(OUT, payload)
    print(f"Wrote -> {OUT}")


if __name__ == "__main__":
    main()

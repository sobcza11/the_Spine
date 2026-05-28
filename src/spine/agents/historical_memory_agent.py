from pathlib import Path

from spine.agents.base_agent_runtime import (
    load_json,
    save_json,
    load_rag_set,
    governed_agent_output,
)


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")
OUT = ROOT / "agents" / "historical_memory_agent.json"


def main():
    analogs = load_json(ROOT / "oraclechambers" / "oc_historical_memory_local.json")
    overlays = load_json(ROOT / "visuals" / "historical_analog_overlays.json")
    citations = load_rag_set("hybrid_combined")[:3]

    top = analogs.get("top_analog", {})
    overlay_count = len(overlays.get("overlays", []))

    synthesis = [
        f"Top historical analog: {top.get('regime', 'unknown')} with similarity {top.get('similarity', 'n/a')}.",
        f"Historical overlay count available: {overlay_count}.",
        "Historical cognition should be used as context, not deterministic prediction.",
        "Analog reasoning should remain subordinate to current cross-asset evidence & governance constraints.",
    ]

    payload = governed_agent_output(
        module="historical-memory-agent",
        agent_name="historical_memory_agent",
        task="historical_continuity_and_analog_reasoning",
        synthesis=synthesis,
        citations=citations,
    )

    save_json(OUT, payload)
    print(f"Wrote -> {OUT}")


if __name__ == "__main__":
    main()

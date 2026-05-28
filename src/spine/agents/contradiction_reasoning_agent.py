from pathlib import Path

from spine.agents.base_agent_runtime import (
    load_json,
    save_json,
    load_rag_set,
    governed_agent_output,
)


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")
OUT = ROOT / "agents" / "contradiction_reasoning_agent.json"


def main():
    contradiction = load_json(ROOT / "oraclechambers" / "oc_contradiction_local.json")
    heatmaps = load_json(ROOT / "visuals" / "contradiction_heatmaps.json")
    citations = load_rag_set("rbl_contextual")[:3]

    max_severity = contradiction.get("max_severity", "n/a")

    synthesis = [
        f"Cross-asset contradiction severity is currently {max_severity}.",
        "Risk appetite should remain conditional until equities, rates, & FX confirm the same regime.",
        "Contradiction cognition should be treated as an executive confidence governor, not a secondary dashboard item.",
        f"Heatmap evidence available: {len(heatmaps.get('heatmap', []))} contradiction pairs.",
    ]

    payload = governed_agent_output(
        module="contradiction-reasoning-agent",
        agent_name="contradiction_agent",
        task="cross_asset_disagreement_reasoning",
        synthesis=synthesis,
        citations=citations,
    )

    save_json(OUT, payload)
    print(f"Wrote -> {OUT}")


if __name__ == "__main__":
    main()

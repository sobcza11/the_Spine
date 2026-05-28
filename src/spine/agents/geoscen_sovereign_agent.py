from pathlib import Path

from spine.agents.base_agent_runtime import (
    load_json,
    save_json,
    load_rag_set,
    governed_agent_output,
)


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")
OUT = ROOT / "agents" / "geoscen_sovereign_agent.json"


def main():
    vectors = load_json(ROOT / "geoscen" / "sovereign_vector_engine.json")
    transmission = load_json(ROOT / "geoscen" / "regional_transmission_systems.json")
    citations = load_rag_set("geoscen_sovereign")[:4]

    country_lines = []
    for item in vectors.get("vectors", []):
        v = item.get("vector", {})
        country_lines.append(
            f"{item.get('country')}: pressure={v.get('pressure')}, fragility={v.get('fragility')}, policy_divergence={v.get('policy_divergence')}"
        )

    synthesis = [
        "GeoScen sovereign cognition is active, but country vectors remain decision-support signals.",
        *country_lines,
        f"Regional transmission links available: {len(transmission.get('transmission_network', []))}.",
        "Sovereign conclusions should remain conditional until source-fed scoring replaces skeleton scores.",
    ]

    payload = governed_agent_output(
        module="geoscen-sovereign-agent",
        agent_name="geoscen_sovereign_agent",
        task="country_level_sovereign_synthesis",
        synthesis=synthesis,
        citations=citations,
    )

    save_json(OUT, payload)
    print(f"Wrote -> {OUT}")


if __name__ == "__main__":
    main()

from pathlib import Path

from spine.agents.base_agent_runtime import (
    load_json,
    save_json,
    load_rag_set,
    governed_agent_output,
)


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")
OUT = ROOT / "agents" / "executive_escalation_agent.json"


def main():
    attention = load_json(ROOT / "oraclechambers" / "oc_attention_routing_local.json")
    final_metric = load_json(ROOT / "oraclechambers" / "oc_final_metric_local.json")
    citations = load_rag_set("rbl_contextual")[:3]

    top = attention.get("top_priority", {})
    score = final_metric.get("scores", {}).get("final_score", "n/a")

    synthesis = [
        f"Top escalation area: {top.get('area', 'unknown')}.",
        f"Escalation reason: {top.get('reason', 'No reason available')}.",
        f"Final deployability score is {score}; use this as a confidence gate, not a standalone approval.",
        "Executive attention should prioritize contradiction resolution before online deployment decisions.",
    ]

    payload = governed_agent_output(
        module="executive-escalation-agent",
        agent_name="executive_escalation_agent",
        task="priority_escalation_cognition",
        synthesis=synthesis,
        citations=citations,
    )

    save_json(OUT, payload)
    print(f"Wrote -> {OUT}")


if __name__ == "__main__":
    main()

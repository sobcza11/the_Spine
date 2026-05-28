from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

CONTEXT_PATH = ROOT / "rbl_agent" / "rbl_grounded_context_bundle.json"
OUT_PATH = ROOT / "rbl_agent" / "langroid_rbl_agent_output.json"


def load_json(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Missing required context bundle: {path}")

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def read_signal_state(payload: dict, fallback: str = "unknown") -> str:
    return payload.get("state") or payload.get("status") or fallback


def run_read_only_rbl_agent(context: dict) -> dict:
    inputs = context.get("inputs", {})

    final_metric = inputs.get("final_metric", {})
    contradiction = inputs.get("contradiction", {})
    attention = inputs.get("attention", {})
    equities = inputs.get("equities", {})
    rates = inputs.get("rates", {})
    fx = inputs.get("fx", {})
    geoscen = inputs.get("geoscen", {})

    final_score = final_metric.get("scores", {}).get("final_score", "n/a")
    max_severity = contradiction.get("max_severity", "n/a")
    top_priority = attention.get("top_priority", {})

    equities_state = read_signal_state(equities)
    rates_state = read_signal_state(rates)
    fx_state = read_signal_state(fx)

    synthesis = [
        f"System confidence is conditionally deployable with a final score of {final_score}.",
        f"Equities are currently reading as {equities_state}, while rates are {rates_state} & FX is {fx_state}.",
        f"The key RBL issue is contradiction severity at {max_severity}; this should govern executive confidence more than any single domain signal.",
        "The platform should not treat risk appetite as fully confirmed until rates, FX, & liquidity signals align with the equity posture.",
        "GeoScen is available for sovereign synthesis, but the current sovereign vectors should remain decision-support inputs, not final institutional conclusions.",
    ]

    return {
        "system": "OracleChambers",
        "module": "langroid-rbl-agent-output",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "agent_name": "rbl_agent",
        "agent_mode": "live_read_only_execution_stub",
        "headline": "RBL agent execution completed: confidence is conditional, not absolute.",
        "synthesis": synthesis,
        "executive_attention": [
            f"Top priority: {top_priority.get('area', 'unknown')} ? {top_priority.get('reason', 'No reason available')}",
            f"Final deployability / confidence score: {final_score}",
            f"Max contradiction severity: {max_severity}",
        ],
        "domain_basis": {
            "equities_state": equities_state,
            "rates_state": rates_state,
            "fx_state": fx_state,
            "geoscen_vectors": geoscen.get("vectors", []),
        },
        "source_payloads": [
            "rbl_grounded_context_bundle.json",
            "oc_final_metric_local.json",
            "oc_contradiction_local.json",
            "oc_attention_routing_local.json",
            "equities_index_plane.json",
            "rates_plane.json",
            "fx_plane.json",
            "sovereign_vector_engine.json",
        ],
        "governance": {
            "agent_generated": True,
            "live_execution_stub": True,
            "read_only": True,
            "llm_writeback_allowed": False,
            "the_spine_mutation_allowed": False,
            "deterministic_payloads_untouched": True,
            "requires_human_review": True,
            "source_payloads_required": True,
        },
    }


def main():
    context = load_json(CONTEXT_PATH)
    output = run_read_only_rbl_agent(context)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    print(f"Wrote live read-only RBL agent output -> {OUT_PATH}")


if __name__ == "__main__":
    main()

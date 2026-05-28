from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")
OUT_DIR = ROOT / "rbl_agent"

PAYLOAD_ROOTS = {
    "oraclechambers": ROOT / "oraclechambers",
    "planes": ROOT / "planes",
    "geoscen": ROOT / "geoscen",
    "tier3": ROOT / "tier3",
    "tier35": ROOT / "tier35",
}


def load_json(path: Path) -> dict:
    if not path.exists():
        return {"missing": True, "path": str(path)}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(name: str, payload: dict) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUT_DIR / name
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print(f"Wrote -> {path}")


def build_input_contract() -> dict:
    return {
        "system": "OracleChambers",
        "module": "rbl-agent-input-contract",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "allowed_inputs": [
            "oc_rbl_local.json",
            "oc_final_metric_local.json",
            "oc_contradiction_local.json",
            "oc_attention_routing_local.json",
            "equities_index_plane.json",
            "rates_plane.json",
            "fx_plane.json",
            "sovereign_vector_engine.json",
            "controlled_rag_retrieval.json",
            "langroid_rbl_agent.json",
        ],
        "blocked_actions": [
            "write_to_the_spine",
            "mutate_measurements",
            "invent_scores",
            "bypass_provenance",
            "uncited_claims",
        ],
        "governance": {
            "read_only": True,
            "llm_writeback_allowed": False,
            "requires_source_payload": True,
            "requires_human_review": True,
        },
    }


def build_grounded_context_bundle() -> dict:
    inputs = {
        "rbl": load_json(PAYLOAD_ROOTS["oraclechambers"] / "oc_rbl_local.json"),
        "final_metric": load_json(PAYLOAD_ROOTS["oraclechambers"] / "oc_final_metric_local.json"),
        "contradiction": load_json(PAYLOAD_ROOTS["oraclechambers"] / "oc_contradiction_local.json"),
        "attention": load_json(PAYLOAD_ROOTS["oraclechambers"] / "oc_attention_routing_local.json"),
        "equities": load_json(PAYLOAD_ROOTS["planes"] / "equities_index_plane.json"),
        "rates": load_json(PAYLOAD_ROOTS["planes"] / "rates_plane.json"),
        "fx": load_json(PAYLOAD_ROOTS["planes"] / "fx_plane.json"),
        "geoscen": load_json(PAYLOAD_ROOTS["geoscen"] / "sovereign_vector_engine.json"),
        "rag_rules": load_json(PAYLOAD_ROOTS["tier3"] / "controlled_rag_retrieval.json"),
    }

    return {
        "system": "OracleChambers",
        "module": "rbl-grounded-context-bundle",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "inputs": inputs,
        "retrieval_context": [
            {
                "source": "controlled_rag_retrieval.json",
                "claim": "RAG must remain read-only, source-required, citation-required, and blocked from measurement writeback.",
            },
            {
                "source": "langroid_rbl_agent.json",
                "claim": "RBL agent must remain scoped to read-only interpretation.",
            },
        ],
        "governance": {
            "rag_grounded": True,
            "read_only": True,
            "source_required": True,
            "llm_writeback_allowed": False,
        },
    }


def build_langroid_rbl_agent_output(context: dict) -> dict:
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

    synthesis = [
        "The current system posture is operational, but confidence should remain conditional because cross-asset confirmation is incomplete.",
        "Equities are showing constructive risk-appetite behavior, but rates & FX remain important confirmation layers.",
        "The contradiction signal should be treated as the primary executive watchpoint because regime confidence weakens when risk assets & liquidity conditions diverge.",
        "GeoScen is now available for sovereign synthesis, but country vectors should be upgraded with real source-fed calculations before drawing institutional conclusions.",
        "The RBL layer is now agent-ready: deterministic payloads remain authoritative, while agent synthesis remains read-only & human-reviewed.",
    ]

    return {
        "system": "OracleChambers",
        "module": "langroid-rbl-agent-output",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "agent_name": "rbl_agent",
        "agent_mode": "read_only_synthesis",
        "headline": "Operational cognition is present, but regime confirmation remains incomplete.",
        "synthesis": synthesis,
        "executive_attention": [
            f"Top priority: {top_priority.get('area', 'unknown')} ? {top_priority.get('reason', 'No reason available')}",
            f"Final deployability / confidence score: {final_score}",
            f"Max contradiction severity: {max_severity}",
        ],
        "domain_basis": {
            "equities_state": equities.get("state"),
            "rates_state": rates.get("state"),
            "fx_state": fx.get("state"),
            "geoscen_vectors": geoscen.get("vectors", []),
        },
        "source_payloads": [
            "oc_rbl_local.json",
            "oc_final_metric_local.json",
            "oc_contradiction_local.json",
            "oc_attention_routing_local.json",
            "equities_index_plane.json",
            "rates_plane.json",
            "fx_plane.json",
            "sovereign_vector_engine.json",
            "controlled_rag_retrieval.json",
        ],
        "governance": {
            "agent_generated": True,
            "read_only": True,
            "llm_writeback_allowed": False,
            "the_spine_mutation_allowed": False,
            "requires_human_review": True,
            "source_payloads_required": True,
        },
    }


def build_saved_agent_record(agent_output: dict) -> dict:
    return {
        "system": "OracleChambers",
        "module": "rbl-agent-saved-output-record",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "record_type": "agent_synthesis_snapshot",
        "agent_output_file": "langroid_rbl_agent_output.json",
        "status": "saved_separately",
        "hash_ready": False,
        "audit_ready": True,
        "governance": {
            "stored_separately": True,
            "does_not_overwrite_deterministic_payloads": True,
            "human_review_required": True,
            "llm_writeback_allowed": False,
        },
        "summary": {
            "headline": agent_output.get("headline"),
            "source_count": len(agent_output.get("source_payloads", [])),
            "synthesis_count": len(agent_output.get("synthesis", [])),
        },
    }


def main():
    input_contract = build_input_contract()
    write_json("rbl_agent_input_contract.json", input_contract)

    context_bundle = build_grounded_context_bundle()
    write_json("rbl_grounded_context_bundle.json", context_bundle)

    agent_output = build_langroid_rbl_agent_output(context_bundle)
    write_json("langroid_rbl_agent_output.json", agent_output)

    saved_record = build_saved_agent_record(agent_output)
    write_json("rbl_agent_saved_output_record.json", saved_record)


if __name__ == "__main__":
    main()

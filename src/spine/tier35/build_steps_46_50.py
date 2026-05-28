from pathlib import Path
from datetime import datetime, timezone
import json


OUT_DIR = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier35"
)


def write_payload(name: str, payload: dict):

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    path = OUT_DIR / f"{name}.json"

    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"Wrote -> {path}")


def agent_payload(
    module: str,
    agent_name: str,
    scope: str,
    source_payloads: list[str],
):

    return {
        "system": "OracleChambers",
        "module": module,
        "agent_name": agent_name,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "status": "operational_skeleton",
        "scope": scope,
        "source_payloads": source_payloads,
        "allowed_actions": [
            "read_payload",
            "summarize",
            "rank_attention",
            "surface_limitations",
        ],
        "blocked_actions": [
            "write_to_the_spine",
            "mutate_measurements",
            "invent_scores",
            "bypass_provenance",
        ],
        "governance": {
            "agent_enabled": True,
            "agent_writeback_allowed": False,
            "the_spine_mutation_allowed": False,
            "requires_source_payload": True,
            "requires_human_review": True,
        },
    }


def main():

    # 46 ? Langroid RBL agent
    write_payload(
        "langroid_rbl_agent",
        agent_payload(
            "langroid-rbl-agent",
            "rbl_agent",
            "read_only_interpretation_agent",
            [
                "oc_rbl_local.json",
                "oc_final_metric_local.json",
                "runtime_health.json",
            ],
        ),
    )

    # 47 ? Langroid contradiction agent
    write_payload(
        "langroid_contradiction_agent",
        agent_payload(
            "langroid-contradiction-agent",
            "contradiction_agent",
            "cross_asset_reasoning_agent",
            [
                "oc_contradiction_local.json",
                "contradiction_heatmaps.json",
                "equities_index_plane.json",
                "rates_plane.json",
                "fx_plane.json",
            ],
        ),
    )

    # 48 ? Langroid Fedspeak agent
    write_payload(
        "langroid_fedspeak_agent",
        agent_payload(
            "langroid-fedspeak-agent",
            "fedspeak_agent",
            "central_bank_interpretation_agent",
            [
                "fedspeak_cognition.json",
                "controlled_rag_retrieval.json",
            ],
        ),
    )

    # 49 ? Langroid GeoScen agent
    write_payload(
        "langroid_geoscen_agent",
        agent_payload(
            "langroid-geoscen-agent",
            "geoscen_agent",
            "sovereign_synthesis_agent",
            [
                "sovereign_canonical_layer.json",
                "sovereign_vector_engine.json",
                "regional_transmission_systems.json",
            ],
        ),
    )

    # 50 ? Executive routing agent
    write_payload(
        "executive_routing_agent",
        agent_payload(
            "executive-routing-agent",
            "executive_routing_agent",
            "priority_escalation_cognition",
            [
                "oc_attention_routing_local.json",
                "confidence_topology.json",
                "runtime_health.json",
            ],
        ),
    )


if __name__ == "__main__":
    main()

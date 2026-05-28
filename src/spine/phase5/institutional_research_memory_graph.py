from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "phase5"
OUT_PATH = OUT_DIR / "institutional_research_memory_graph.json"


MEMORY_GRAPH_NODES = [
    "hypothesis_nodes",
    "signal_candidate_nodes",
    "validation_result_nodes",
    "failed_experiment_nodes",
    "thesis_nodes",
    "ontology_change_nodes",
    "causal_claim_nodes",
    "peer_review_nodes",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "institutional-research-memory-graph",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "institutional_research_memory_graph_enabled": True,

        "memory_graph_nodes": MEMORY_GRAPH_NODES,
        "memory_graph_node_count": len(MEMORY_GRAPH_NODES),

        "memory_graph_objective": (
            "Track persistent institutional research lineage across hypotheses, signal "
            "candidates, validation results, failed experiments, theses, ontology changes, "
            "causal claims, and peer reviews."
        ),

        "memory_graph_contract": {
            "research_lineage_required": True,
            "failed_experiments_preserved": True,
            "validation_results_linked": True,
            "causal_claims_tracked": True,
            "human_review_required": True,
        },

        "governance": {
            "research_memory_graph_governed": True,
            "memory_write_requires_review": True,
            "unreviewed_promotion_forbidden": True,
            "llm_writeback_allowed": False,
            "audit_trail_required": True,
        },
    }

    OUT_PATH.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )

    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()

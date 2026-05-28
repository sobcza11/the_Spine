from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "tier7"
OUT_PATH = OUT_DIR / "global_macro_knowledge_graph.json"


GRAPH_DOMAINS = [
    "macro_entities",
    "policy_entities",
    "sovereign_entities",
    "asset_class_entities",
    "liquidity_entities",
    "historical_regime_entities",
    "narrative_entities",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "global-macro-knowledge-graph",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "knowledge_graph_enabled": True,

        "graph_domains": GRAPH_DOMAINS,

        "graph_domain_count": len(GRAPH_DOMAINS),

        "graph_objective": (
            "Create a governed global macro knowledge graph connecting macro entities, "
            "policy entities, sovereign entities, asset-class entities, liquidity entities, "
            "historical regimes, and narratives."
        ),

        "graph_contract": {
            "entity_lineage_required": True,
            "relationship_provenance_required": True,
            "cross_domain_links_supported": True,
            "historical_context_supported": True,
            "human_review_required": True,
        },

        "governance": {
            "knowledge_graph_governed": True,
            "llm_writeback_allowed": False,
            "unverified_edges_allowed": False,
            "mutation_requires_authorization": True,
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

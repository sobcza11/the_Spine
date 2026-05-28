from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")
OUT_DIR = ROOT / "tier6"
OUT_PATH = OUT_DIR / "institutional_reasoning_graph.json"

REASONING_EDGES = [
    {
        "source": "rates_pressure",
        "target": "liquidity_tightening",
        "relationship": "increases",
    },
    {
        "source": "liquidity_tightening",
        "target": "credit_stress",
        "relationship": "raises",
    },
    {
        "source": "credit_stress",
        "target": "equity_breadth_weakness",
        "relationship": "pressures",
    },
    {
        "source": "equity_breadth_weakness",
        "target": "contradiction_severity",
        "relationship": "raises",
    },
    {
        "source": "contradiction_severity",
        "target": "executive_confidence",
        "relationship": "reduces",
    },
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    nodes = sorted(
        set([x["source"] for x in REASONING_EDGES] + [x["target"] for x in REASONING_EDGES])
    )

    payload = {
        "system": "IsoVector",
        "module": "institutional-reasoning-graph",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "reasoning_graph_enabled": True,
        "nodes": nodes,
        "edges": REASONING_EDGES,
        "node_count": len(nodes),
        "edge_count": len(REASONING_EDGES),
        "governance": {
            "causal_claims_require_review": True,
            "reasoning_audit_required": True,
            "graph_is_decision_support": True,
            "llm_writeback_allowed": False,
        },
    }

    OUT_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()

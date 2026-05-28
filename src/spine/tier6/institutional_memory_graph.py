from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "tier6"
OUT_PATH = OUT_DIR / "institutional_memory_graph.json"


MEMORY_NODES = [
    {
        "node": "2018_qt",
        "category": "historical_regime",
    },
    {
        "node": "2022_inflation_shock",
        "category": "historical_regime",
    },
    {
        "node": "usd_liquidity_pressure",
        "category": "macro_signal",
    },
    {
        "node": "regional_sovereign_stress",
        "category": "sovereign_signal",
    },
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "institutional-memory-graph",
        "generated_utc": datetime.now(
            timezone.utc
        ).isoformat(),

        "institutional_memory_graph_enabled": True,

        "memory_nodes": MEMORY_NODES,

        "memory_node_count": len(
            MEMORY_NODES
        ),

        "governance": {
            "memory_graph_read_only": True,
            "cross_regime_traceability": True,
            "memory_audit_required": True,
            "llm_writeback_allowed": False,
        },
    }

    OUT_PATH.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )

    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()

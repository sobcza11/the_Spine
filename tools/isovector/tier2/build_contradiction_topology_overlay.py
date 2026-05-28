from pathlib import Path
import json
from datetime import datetime

ROOT = Path.cwd()
OUT = ROOT / "data" / "serving" / "isovector" / "tier2"
OUT.mkdir(parents=True, exist_ok=True)

topology = {
    "artifact": "contradiction_topology_overlay",
    "version": "v1",
    "created_at": datetime.now().isoformat(timespec="seconds"),
    "definition": "Cross-plane disagreement map for institutional regime conflict detection.",
    "nodes": [
        "FX",
        "RATES",
        "C_FLOW",
        "EQUITIES_SECTOR",
        "EQUITIES_INDUSTRY"
    ],
    "edges": [
        {"source": "FX", "target": "RATES", "relationship": "currency-rate policy tension"},
        {"source": "RATES", "target": "C_FLOW", "relationship": "inflation-duration contradiction"},
        {"source": "C_FLOW", "target": "EQUITIES_SECTOR", "relationship": "commodity-margin pressure"},
        {"source": "EQUITIES_SECTOR", "target": "EQUITIES_INDUSTRY", "relationship": "sector-industry breadth divergence"},
        {"source": "FX", "target": "C_FLOW", "relationship": "dollar-commodity pressure"}
    ],
    "governance": {
        "ai_generated_edges_allowed": False,
        "deterministic_topology_required": True,
        "executive_overlay_allowed": True
    }
}

(OUT / "contradiction_topology_overlay_v1.json").write_text(
    json.dumps(topology, indent=2),
    encoding="utf-8"
)

print("[OK] contradiction topology overlay built")


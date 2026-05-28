from pathlib import Path
import json
from datetime import datetime

ROOT = Path.cwd()
OUT = ROOT / "data" / "serving" / "isovector" / "tier2"
OUT.mkdir(parents=True, exist_ok=True)

routing = {
    "artifact": "executive_routing_layer",
    "version": "v1",
    "created_at": datetime.now().isoformat(timespec="seconds"),
    "purpose": "Prioritize institutional attention across cognition planes.",
    "routing_rules": [
        {
            "condition": "high contradiction",
            "priority": 1,
            "route_to": "Executive Attention"
        },
        {
            "condition": "liquidity deterioration",
            "priority": 2,
            "route_to": "Risk Posture"
        },
        {
            "condition": "historical analog activation",
            "priority": 3,
            "route_to": "Analog Memory"
        },
        {
            "condition": "broad dispersion",
            "priority": 4,
            "route_to": "Feature Drivers"
        }
    ],
    "governance": {
        "routing_can_prioritize": True,
        "routing_can_mutate_truth": False,
        "routing_requires_signal_basis": True
    }
}

(OUT / "executive_routing_layer_v1.json").write_text(
    json.dumps(routing, indent=2),
    encoding="utf-8"
)

print("[OK] executive routing layer built")


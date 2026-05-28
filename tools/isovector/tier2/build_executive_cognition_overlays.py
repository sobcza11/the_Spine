from pathlib import Path
import json
from datetime import datetime

ROOT = Path.cwd()
OUT = ROOT / "data" / "serving" / "isovector" / "tier2"
OUT.mkdir(parents=True, exist_ok=True)

planes = ["FX", "RATES", "C_FLOW", "EQUITIES_SECTOR", "EQUITIES_INDUSTRY"]

payload = {
    "artifact": "executive_cognition_overlays",
    "version": "v1",
    "created_at": datetime.now().isoformat(timespec="seconds"),
    "planes": {
        plane: {
            "executive_state": "active",
            "summary_mode": "institutional",
            "decision_bias": "monitor",
            "risk_posture": "watchful",
            "watch_items": [],
            "governance": {
                "zt_mutation_allowed": False,
                "ai_assisted": True,
                "deterministic_truth_preserved": True
            }
        }
        for plane in planes
    }
}

(OUT / "executive_cognition_overlays_v1.json").write_text(
    json.dumps(payload, indent=2),
    encoding="utf-8"
)

print("[OK] executive cognition overlays built")


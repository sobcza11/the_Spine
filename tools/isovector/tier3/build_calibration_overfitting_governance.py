from pathlib import Path
import json
from datetime import datetime

ROOT = Path.cwd()
OUT = ROOT / "data" / "serving" / "isovector" / "tier3" / "calibration"
OUT.mkdir(parents=True, exist_ok=True)

payload = {
    "artifact": "calibration_overfitting_governance",
    "version": "v1",
    "created_at": datetime.now().isoformat(timespec="seconds"),
    "controls": {
        "confidence_calibration": {
            "enabled": True,
            "purpose": "Ensure executive confidence language does not exceed signal support."
        },
        "feature_concentration_monitor": {
            "enabled": True,
            "max_single_feature_dominance": 0.60
        },
        "narrative_stability_check": {
            "enabled": True,
            "purpose": "Prevent RBL overreaction to small signal changes."
        },
        "contradiction_balance_check": {
            "enabled": True,
            "purpose": "Ensure cross-plane disagreement remains visible."
        }
    },
    "governance": {
        "ai_may_adjust_tone": True,
        "ai_may_adjust_truth": False,
        "zt_mutation_allowed": False,
        "overconfidence_flag_required": True
    }
}

(OUT / "calibration_overfitting_governance_v1.json").write_text(
    json.dumps(payload, indent=2),
    encoding="utf-8"
)

print("[OK] calibration & overfitting governance built")


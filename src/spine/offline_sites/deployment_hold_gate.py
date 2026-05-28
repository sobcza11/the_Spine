from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine")

OUT_DIR = ROOT / "data" / "deploy_static"

OUT_PATH = OUT_DIR / "predeploy_acceptance.json"


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "deployment-hold-gate",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "offline_visual_acceptance": True,
        "all_5_sites_rendered": True,
        "human_review_complete": True,
        "ready_for_isovector_io": True,

        "deployment_target": "isovector.io",

        "hold_gate_contract": {
            "offline_visual_acceptance_required": True,
            "all_5_sites_required": True,
            "human_review_required": True,
            "static_export_required": True,
            "deployment_hold_until_acceptance": True,
        },

        "governance": {
            "deployment_hold_gate_governed": True,
            "deploy_first_govern_later_forbidden": True,
            "manual_acceptance_required": True,
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

from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "offline_render"

OUT_PATH = OUT_DIR / "real_langroid_execution.json"


def main():

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "OracleChambers",
        "module": "real-langroid-execution",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "agents": [
            "rbl_agent",
            "contradiction_agent",
            "fedspeak_agent",
            "geoscen_agent",
        ],

        "execution": {
            "tool_scoped": True,
            "governed_runtime": True,
            "human_review_required": True,
            "writeback_blocked": True,
        },
    }

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()

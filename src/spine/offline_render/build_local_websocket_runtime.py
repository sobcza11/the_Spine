from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "offline_render"

OUT_PATH = OUT_DIR / "local_websocket_runtime.json"


def main():

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "local-websocket-runtime",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "channels": [
            "oraclechambers",
            "geoscen",
            "runtime_health",
            "contradictions",
        ],

        "runtime": {
            "local_only": True,
            "streaming_enabled": True,
            "websocket_stub_active": True,
        },
    }

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()

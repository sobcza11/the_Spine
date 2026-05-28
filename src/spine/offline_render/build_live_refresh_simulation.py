from pathlib import Path
from datetime import datetime, timezone
import json
import time


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "offline_render"

OUT_PATH = OUT_DIR / "live_refresh_simulation.json"


def main():

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    for i in range(5):

        payload = {
            "system": "IsoVector",
            "module": "live-refresh-simulation",
            "iteration": i,
            "generated_utc": datetime.now(timezone.utc).isoformat(),
            "status": "runtime_refresh_active",
        }

        with open(OUT_PATH, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

        print(f"Refresh iteration -> {i}")

        time.sleep(2)


if __name__ == "__main__":
    main()

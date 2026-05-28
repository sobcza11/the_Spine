from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "offline_render"

OUT_PATH = OUT_DIR / "real_rag_integration.json"


def main():

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "OracleChambers",
        "module": "real-rag-integration",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "rag": {
            "vector_store_required": True,
            "citation_required": True,
            "read_only_retrieval": True,
            "source_grounding_required": True,
        },
    }

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()

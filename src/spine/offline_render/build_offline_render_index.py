from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "offline_render"
OUT_PATH = OUT_DIR / "offline_render_index.json"


SOURCE_DIRS = [
    ROOT / "oraclechambers",
    ROOT / "oraclechambers_ai",
    ROOT / "tier3",
    ROOT / "tier35",
    ROOT / "tier4",
    ROOT / "tier5",
    ROOT / "geoscen",
    ROOT / "planes",
    ROOT / "visuals",
    ROOT / "final_batch",
]


def load_json(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main():

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    artifacts = []

    for directory in SOURCE_DIRS:

        if not directory.exists():
            continue

        for path in sorted(directory.glob("*.json")):

            data = load_json(path)

            artifacts.append({
                "path": str(path),
                "file": path.name,
                "system": data.get("system"),
                "module": data.get("module"),
                "plane": data.get("plane"),
                "generated_utc": data.get("generated_utc"),
                "status": data.get("status", "available"),
            })

    payload = {
        "system": "IsoVector",
        "module": "offline-render-index",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "artifact_count": len(artifacts),
        "artifacts": artifacts,

        "governance": {
            "offline_only": True,
            "deployment_target": "local",
            "online_deployment_allowed": False,
        },
    }

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"Wrote -> {OUT_PATH}")
    print(f"Artifact count -> {len(artifacts)}")


if __name__ == "__main__":
    main()

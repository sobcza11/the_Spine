from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


def build_full_tier_orchestration_manifest_v1():
    root = Path.cwd()
    registry_path = root / "data" / "registry" / "tier_status_registry_v1.parquet"
    lineage_path = root / "data" / "registry" / "module_lineage_registry_v1.parquet"
    out = root / "data" / "registry"
    out.mkdir(parents=True, exist_ok=True)

    if not registry_path.exists():
        raise FileNotFoundError(f"Missing registry: {registry_path}")

    df = pd.read_parquet(registry_path)

    manifest = {
        "component": "full_tier_orchestration_manifest_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "execution_order": [],
        "status": "full_tier_orchestration_manifest_complete",
    }

    tier_order = ["Tier 1", "Tier 2", "Tier 3", "Tier 4", "Unclassified"]

    for tier in tier_order:
        g = df[df["tier"] == tier].copy()
        if g.empty:
            continue

        manifest["execution_order"].append({
            "tier": tier,
            "module_count": int(len(g)),
            "modules": g[["component", "status", "source_file"]].to_dict(orient="records"),
        })

    if lineage_path.exists():
        lineage = pd.read_parquet(lineage_path)
        manifest["lineage_registry_attached"] = True
        manifest["lineage_count"] = int(len(lineage))
    else:
        manifest["lineage_registry_attached"] = False
        manifest["lineage_count"] = 0

    with open(out / "full_tier_orchestration_manifest_v1.json", "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    print("Full Tier Orchestration Manifest complete")
    print("Tiers:", len(manifest["execution_order"]))
    print("OUTPUT:", out / "full_tier_orchestration_manifest_v1.json")


if __name__ == "__main__":
    build_full_tier_orchestration_manifest_v1()

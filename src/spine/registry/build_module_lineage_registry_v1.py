from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


LINEAGE_RULES = {
    "finstate": ["I2", "GeoScen", "IV[t]"],
    "i2": ["FinStateIV", "FinCore", "GeoScen"],
    "cot": ["GeoScen", "IV[t]"],
    "rates": ["GeoScen", "IV[t]"],
    "vinv": ["GeoScen", "IV[t]"],
    "debasement": ["GeoScen", "IV[t]"],
    "geoscen": ["IV[t]", "Executive Synthesis"],
    "runtime": ["Institutional Recursive Operating System"],
    "adaptive": ["Institutional Recursive Operating System"],
}


def infer_children(component: str, source_file: str):
    text = f"{component} {source_file}".lower()
    children = []

    for key, targets in LINEAGE_RULES.items():
        if key in text:
            children.extend(targets)

    return sorted(set(children))


def build_module_lineage_registry_v1():
    root = Path.cwd()
    registry_path = root / "data" / "registry" / "tier_status_registry_v1.parquet"
    out = root / "data" / "registry"
    out.mkdir(parents=True, exist_ok=True)

    if not registry_path.exists():
        raise FileNotFoundError(f"Missing registry: {registry_path}")

    df = pd.read_parquet(registry_path)

    rows = []

    for _, r in df.iterrows():
        component = r.get("component")
        source_file = r.get("source_file")
        children = infer_children(str(component), str(source_file))

        rows.append({
            "component": component,
            "tier": r.get("tier"),
            "source_file": source_file,
            "propagates_to": children,
            "propagation_count": len(children),
            "lineage_status": "lineage_mapped" if children else "lineage_unmapped",
        })

    lineage = pd.DataFrame(rows)

    summary = {
        "component": "module_lineage_registry_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "lineage_rows": int(len(lineage)),
        "mapped_count": int((lineage["lineage_status"] == "lineage_mapped").sum()) if not lineage.empty else 0,
        "status": "module_lineage_registry_complete",
    }

    lineage.to_parquet(out / "module_lineage_registry_v1.parquet", index=False)
    lineage.to_json(out / "module_lineage_registry_v1.json", orient="records", indent=2)

    with open(out / "module_lineage_registry_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Module Lineage Registry complete")
    print("Mapped:", summary["mapped_count"])
    print("SUMMARY:", out / "module_lineage_registry_summary_v1.json")


if __name__ == "__main__":
    build_module_lineage_registry_v1()

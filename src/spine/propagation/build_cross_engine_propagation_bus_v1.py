from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


ENGINE_MAP = {
    "finstate": ["I2", "GeoScen", "IV[t]"],
    "i2": ["FinStateIV", "GeoScen", "VinV", "IV[t]"],
    "cot": ["GeoScen", "IV[t]"],
    "rates": ["GeoScen", "IV[t]"],
    "vinv": ["GeoScen", "IV[t]"],
    "debasement": ["GeoScen", "IV[t]"],
    "geoscen": ["IV[t]", "Executive Synthesis"],
    "runtime": ["Institutional Runtime"],
    "adaptive": ["Institutional Runtime"],
}


def infer_targets(component, source_file):
    text = f"{component} {source_file}".lower()
    targets = []
    for k, v in ENGINE_MAP.items():
        if k in text:
            targets.extend(v)
    return sorted(set(targets))


def build_cross_engine_propagation_bus_v1():
    root = Path.cwd()
    src = root / "data" / "propagation" / "recursive_state_memory_v1.parquet"
    out = root / "data" / "propagation"
    out.mkdir(parents=True, exist_ok=True)

    if not src.exists():
        raise FileNotFoundError(f"Missing state memory: {src}")

    df = pd.read_parquet(src)
    edges = []

    for _, r in df.iterrows():
        targets = infer_targets(r["component"], r["source_file"])
        for target in targets:
            edges.append({
                "source_component": r["component"],
                "target_engine": target,
                "source_state": r["state"],
                "source_pressure": r["pressure"],
                "edge_type": "recursive_propagation",
            })

    bus = pd.DataFrame(edges)

    if not bus.empty:
        for c in ["source_component", "target_engine", "source_state", "edge_type"]:
            bus[c] = bus[c].fillna("").astype(str)
        bus["source_pressure"] = pd.to_numeric(bus["source_pressure"], errors="coerce")

    summary = {
        "component": "cross_engine_propagation_bus_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "edge_count": int(len(bus)),
        "target_count": int(bus["target_engine"].nunique()) if not bus.empty else 0,
        "status": "cross_engine_propagation_bus_complete",
    }

    bus.to_parquet(out / "cross_engine_propagation_bus_v1.parquet", index=False)
    bus.to_json(out / "cross_engine_propagation_bus_v1.json", orient="records", indent=2)

    with open(out / "cross_engine_propagation_bus_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Cross-Engine Propagation Bus complete")
    print("Edges:", summary["edge_count"])
    print("Targets:", summary["target_count"])


if __name__ == "__main__":
    build_cross_engine_propagation_bus_v1()

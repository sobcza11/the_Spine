from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


def build_institutional_recursive_coordination_layer_v1():
    root = Path.cwd()
    contagion_path = root / "data" / "propagation" / "recursive_contagion_engine_v1.parquet"
    registry_path = root / "data" / "propagation" / "unified_recursive_state_registry_v1.json"
    out = root / "data" / "propagation"
    out.mkdir(parents=True, exist_ok=True)

    if not contagion_path.exists():
        raise FileNotFoundError(f"Missing contagion engine: {contagion_path}")
    if not registry_path.exists():
        raise FileNotFoundError(f"Missing unified registry: {registry_path}")

    contagion = pd.read_parquet(contagion_path)
    registry = json.loads(registry_path.read_text(encoding="utf-8"))

    coordination = (
        contagion.groupby("target_engine", as_index=False)
        .agg(
            coordinated_pressure=("recursive_contagion_pressure", "mean"),
            edge_count=("source_component", "count"),
            source_components=("source_component", lambda x: sorted(set(x))),
        )
    )

    coordination["coordinated_pressure"] = coordination["coordinated_pressure"].round(4)

    final = {
        "component": "institutional_recursive_coordination_layer_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "coordinated_target_count": int(len(coordination)),
        "average_coordinated_pressure": round(float(coordination["coordinated_pressure"].mean()), 4) if not coordination.empty else None,
        "unified_registry_attached": True,
        "memory_component_count": registry.get("memory_component_count"),
        "coordination_targets": coordination.to_dict(orient="records"),
        "status": "institutional_recursive_coordination_layer_complete",
    }

    coordination.to_parquet(out / "institutional_recursive_coordination_layer_v1.parquet", index=False)
    coordination.to_json(out / "institutional_recursive_coordination_layer_v1.json", orient="records", indent=2)

    with open(out / "institutional_recursive_coordination_layer_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(final, f, indent=2)

    print("Institutional Recursive Coordination Layer complete")
    print("Targets:", final["coordinated_target_count"])
    print("Avg coordinated pressure:", final["average_coordinated_pressure"])


if __name__ == "__main__":
    build_institutional_recursive_coordination_layer_v1()

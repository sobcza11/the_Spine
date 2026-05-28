from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


def build_recursive_contagion_engine_v1():
    root = Path.cwd()
    bus_path = root / "data" / "propagation" / "cross_engine_propagation_bus_v1.parquet"
    condition_path = root / "data" / "propagation" / "transition_state_conditioning_layer_v1.parquet"
    out = root / "data" / "propagation"
    out.mkdir(parents=True, exist_ok=True)

    if not bus_path.exists():
        raise FileNotFoundError(f"Missing bus: {bus_path}")
    if not condition_path.exists():
        raise FileNotFoundError(f"Missing conditioning layer: {condition_path}")

    bus = pd.read_parquet(bus_path)
    cond = pd.read_parquet(condition_path)[["target_engine", "conditioning_weight"]]

    df = bus.merge(cond, on="target_engine", how="left")
    df["source_pressure"] = pd.to_numeric(df["source_pressure"], errors="coerce").fillna(0)
    df["conditioning_weight"] = pd.to_numeric(df["conditioning_weight"], errors="coerce").fillna(0.25)
    df["recursive_contagion_pressure"] = (df["source_pressure"] * df["conditioning_weight"]).round(4)

    summary = {
        "component": "recursive_contagion_engine_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "edge_count": int(len(df)),
        "average_contagion_pressure": round(float(df["recursive_contagion_pressure"].mean()), 4) if not df.empty else None,
        "status": "recursive_contagion_engine_complete",
    }

    df.to_parquet(out / "recursive_contagion_engine_v1.parquet", index=False)
    df.to_json(out / "recursive_contagion_engine_v1.json", orient="records", indent=2)

    with open(out / "recursive_contagion_engine_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Recursive Contagion Engine complete")
    print("Edges:", summary["edge_count"])
    print("Avg contagion:", summary["average_contagion_pressure"])


if __name__ == "__main__":
    build_recursive_contagion_engine_v1()

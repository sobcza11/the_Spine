from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd
import numpy as np


def classify(x):
    if x >= 0.70: return "systemic_recursive_escalation"
    if x >= 0.55: return "fragile_recursive_escalation"
    if x >= 0.40: return "elevated_recursive_escalation"
    if x >= 0.25: return "watch_recursive_escalation"
    return "stable_recursive_escalation"


def build_escalation_synchronization_engine_v1():
    root = Path.cwd()
    src = root / "data" / "propagation" / "cross_engine_propagation_bus_v1.parquet"
    out = root / "data" / "propagation"
    out.mkdir(parents=True, exist_ok=True)

    if not src.exists():
        raise FileNotFoundError(f"Missing propagation bus: {src}")

    bus = pd.read_parquet(src)
    bus["source_pressure"] = pd.to_numeric(bus["source_pressure"], errors="coerce").fillna(0)

    sync = (
        bus.groupby("target_engine", as_index=False)
        .agg(
            recursive_pressure=("source_pressure", "mean"),
            signal_count=("source_component", "count"),
            source_components=("source_component", lambda x: sorted(set(x))),
        )
    )

    sync["recursive_pressure"] = sync["recursive_pressure"].round(4)
    sync["recursive_escalation_state"] = sync["recursive_pressure"].apply(classify)

    summary = {
        "component": "escalation_synchronization_engine_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "target_count": int(len(sync)),
        "average_recursive_pressure": round(float(sync["recursive_pressure"].mean()), 4) if not sync.empty else None,
        "status": "escalation_synchronization_engine_complete",
    }

    sync.to_parquet(out / "escalation_synchronization_engine_v1.parquet", index=False)
    sync.to_json(out / "escalation_synchronization_engine_v1.json", orient="records", indent=2)

    with open(out / "escalation_synchronization_engine_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Escalation Synchronization Engine complete")
    print("Targets:", summary["target_count"])
    print("Average pressure:", summary["average_recursive_pressure"])


if __name__ == "__main__":
    build_escalation_synchronization_engine_v1()

from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


def make_json_safe_records(df: pd.DataFrame):
    safe = df.copy()

    for col in safe.columns:
        safe[col] = safe[col].apply(
            lambda x: x.tolist() if hasattr(x, "tolist") else x
        )

    return safe.to_dict(orient="records")


def build_unified_recursive_state_registry_v1():
    root = Path.cwd()
    memory_path = root / "data" / "propagation" / "recursive_state_memory_v1.parquet"
    sync_path = root / "data" / "propagation" / "escalation_synchronization_engine_v1.parquet"
    out = root / "data" / "propagation"
    out.mkdir(parents=True, exist_ok=True)

    if not memory_path.exists():
        raise FileNotFoundError(f"Missing memory: {memory_path}")
    if not sync_path.exists():
        raise FileNotFoundError(f"Missing sync: {sync_path}")

    memory = pd.read_parquet(memory_path)
    sync = pd.read_parquet(sync_path)

    registry = {
        "component": "unified_recursive_state_registry_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "memory_component_count": int(memory["component"].nunique()) if not memory.empty else 0,
        "synchronized_target_count": int(sync["target_engine"].nunique()) if not sync.empty else 0,
        "recursive_state_targets": make_json_safe_records(sync),
        "status": "unified_recursive_state_registry_complete",
    }

    with open(out / "unified_recursive_state_registry_v1.json", "w", encoding="utf-8") as f:
        json.dump(registry, f, indent=2)

    print("Unified Recursive State Registry complete")
    print("Memory components:", registry["memory_component_count"])
    print("Targets:", registry["synchronized_target_count"])


if __name__ == "__main__":
    build_unified_recursive_state_registry_v1()

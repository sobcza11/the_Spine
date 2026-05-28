from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


def build_narrative_state_recursive_memory_v1():
    root = Path.cwd()
    src = root / "data" / "narrative" / "semantic_contagion_propagation_v1.parquet"
    out = root / "data" / "narrative"
    out.mkdir(parents=True, exist_ok=True)

    if not src.exists():
        raise FileNotFoundError(f"Missing semantic contagion propagation: {src}")

    df = pd.read_parquet(src).copy()

    memory = {
        "component": "narrative_state_recursive_memory_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "target_count": int(len(df)),
        "semantic_memory_targets": df.to_dict(orient="records"),
        "status": "narrative_state_recursive_memory_complete",
    }

    with open(out / "narrative_state_recursive_memory_v1.json", "w", encoding="utf-8") as f:
        json.dump(memory, f, indent=2)

    print("Narrative-State Recursive Memory complete")
    print("Targets:", memory["target_count"])


if __name__ == "__main__":
    build_narrative_state_recursive_memory_v1()

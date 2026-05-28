from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


def build_institutional_semantic_runtime_v1():
    root = Path.cwd()
    semantic_path = root / "data" / "narrative" / "semantic_contagion_propagation_v1.parquet"
    memory_path = root / "data" / "narrative" / "narrative_state_recursive_memory_v1.json"
    out = root / "data" / "narrative"
    out.mkdir(parents=True, exist_ok=True)

    if not semantic_path.exists():
        raise FileNotFoundError(f"Missing semantic contagion: {semantic_path}")
    if not memory_path.exists():
        raise FileNotFoundError(f"Missing narrative memory: {memory_path}")

    semantic = pd.read_parquet(semantic_path)
    memory = json.loads(memory_path.read_text(encoding="utf-8"))

    runtime = {
        "component": "institutional_semantic_runtime_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "semantic_target_count": int(len(semantic)),
        "average_semantic_pressure": round(float(semantic["semantic_contagion_pressure"].mean()), 4) if not semantic.empty else None,
        "transformer_inference_enabled": False,
        "semantic_override_allowed": False,
        "memory_attached": True,
        "memory_target_count": memory.get("target_count"),
        "runtime_status": "semantic_runtime_scaffold_active",
        "status": "institutional_semantic_runtime_complete",
    }

    with open(out / "institutional_semantic_runtime_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(runtime, f, indent=2)

    print("Institutional Semantic Runtime complete")
    print("Semantic targets:", runtime["semantic_target_count"])
    print("Transformer inference enabled:", runtime["transformer_inference_enabled"])


if __name__ == "__main__":
    build_institutional_semantic_runtime_v1()

from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["long_history_state_compression", 0.41, 0.24],
    ["recursive_memory_reduction", 0.40, 0.22],
    ["regime_signature_encoding", 0.42, 0.22],
    ["persistence_memory_filter", 0.39, 0.18],
    ["compression_governance_control", 0.43, 0.14],
]

def classify(x):
    if x >= 0.70: return "systemic_memory_compression"
    if x >= 0.55: return "fragile_memory_compression"
    if x >= 0.40: return "elevated_memory_compression"
    if x >= 0.25: return "watch_memory_compression"
    return "stable_memory_compression"

def build_recursive_memory_compression_v1():
    root = Path.cwd()
    out = root / "data" / "isovector" / "tier2"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "recursive_memory_compression_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "recursive_memory_compression_pressure": pressure,
        "recursive_memory_compression_state": state,
        "active_components": df["component"].tolist(),
        "status": "recursive_memory_compression_complete",
    }

    df.to_parquet(out / "recursive_memory_compression_v1.parquet", index=False)
    df.to_json(out / "recursive_memory_compression_v1.json", orient="records", indent=2)

    with open(out / "recursive_memory_compression_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Recursive Memory Compression complete")
    print("Pressure:", pressure)
    print("State:", state)
    print("SUMMARY:", out / "recursive_memory_compression_summary_v1.json")
    print("Summary:", summary)

if __name__ == "__main__":
    build_recursive_memory_compression_v1()

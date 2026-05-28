from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["recursive_cluster_assignment", 0.41, 0.24],
    ["latent_state_grouping", 0.42, 0.24],
    ["cluster_stability_memory", 0.39, 0.18],
    ["cross_domain_cluster_alignment", 0.40, 0.20],
    ["cluster_governance_filter", 0.38, 0.14],
]

def classify(x):
    if x >= 0.70: return "systemic_state_clustering"
    if x >= 0.55: return "fragile_state_clustering"
    if x >= 0.40: return "elevated_state_clustering"
    if x >= 0.25: return "watch_state_clustering"
    return "stable_state_clustering"

def build_recursive_state_clustering_v1():
    root = Path.cwd()
    out = root / "data" / "isovector" / "tier2"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "recursive_state_clustering_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "recursive_state_clustering_pressure": pressure,
        "recursive_state_clustering_state": state,
        "active_components": df["component"].tolist(),
        "status": "recursive_state_clustering_complete",
    }

    df.to_parquet(out / "recursive_state_clustering_v1.parquet", index=False)
    df.to_json(out / "recursive_state_clustering_v1.json", orient="records", indent=2)

    with open(out / "recursive_state_clustering_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Recursive State Clustering complete")
    print("Pressure:", pressure)
    print("State:", state)
    print("SUMMARY:", out / "recursive_state_clustering_summary_v1.json")
    print("Summary:", summary)

if __name__ == "__main__":
    build_recursive_state_clustering_v1()

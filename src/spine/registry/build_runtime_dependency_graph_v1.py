from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


def build_runtime_dependency_graph_v1():
    root = Path.cwd()
    lineage_path = root / "data" / "registry" / "module_lineage_registry_v1.parquet"
    out = root / "data" / "registry"
    out.mkdir(parents=True, exist_ok=True)

    if not lineage_path.exists():
        raise FileNotFoundError(f"Missing lineage registry: {lineage_path}")

    lineage = pd.read_parquet(lineage_path)

    edges = []

    for _, r in lineage.iterrows():
        source = r["component"]
        targets = r["propagates_to"]

        if isinstance(targets, str):
            try:
                targets = json.loads(targets)
            except Exception:
                targets = []

        for target in targets:
            edges.append({
                "source": source,
                "target": target,
                "edge_type": "recursive_dependency",
            })

    graph = {
        "component": "runtime_dependency_graph_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "node_count": int(lineage["component"].nunique()),
        "edge_count": int(len(edges)),
        "edges": edges,
        "status": "runtime_dependency_graph_complete",
    }

    with open(out / "runtime_dependency_graph_v1.json", "w", encoding="utf-8") as f:
        json.dump(graph, f, indent=2)

    pd.DataFrame(edges).to_parquet(out / "runtime_dependency_graph_edges_v1.parquet", index=False)

    print("Runtime Dependency Graph complete")
    print("Edges:", graph["edge_count"])
    print("OUTPUT:", out / "runtime_dependency_graph_v1.json")


if __name__ == "__main__":
    build_runtime_dependency_graph_v1()

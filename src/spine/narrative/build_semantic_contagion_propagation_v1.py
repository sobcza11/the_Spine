from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


def build_semantic_contagion_propagation_v1():
    root = Path.cwd()
    src = root / "data" / "narrative" / "transformer_escalation_layer_v1.parquet"
    out = root / "data" / "narrative"
    out.mkdir(parents=True, exist_ok=True)

    if not src.exists():
        raise FileNotFoundError(f"Missing transformer escalation layer: {src}")

    df = pd.read_parquet(src).copy()

    semantic = (
        df.groupby("target_engine", as_index=False)
        .agg(
            semantic_contagion_pressure=("transformer_escalation_pressure", "mean"),
            semantic_edge_count=("source_file", "count"),
            dominant_semantic_state=("transformer_escalation_state", lambda x: x.mode().iloc[0] if len(x.mode()) else None),
        )
    )

    semantic["semantic_contagion_pressure"] = semantic["semantic_contagion_pressure"].round(4)

    summary = {
        "component": "semantic_contagion_propagation_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "target_count": int(len(semantic)),
        "average_semantic_contagion_pressure": round(float(semantic["semantic_contagion_pressure"].mean()), 4) if not semantic.empty else None,
        "status": "semantic_contagion_propagation_complete",
    }

    semantic.to_parquet(out / "semantic_contagion_propagation_v1.parquet", index=False)
    semantic.to_json(out / "semantic_contagion_propagation_v1.json", orient="records", indent=2)

    with open(out / "semantic_contagion_propagation_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Semantic Contagion Propagation complete")
    print("Targets:", summary["target_count"])


if __name__ == "__main__":
    build_semantic_contagion_propagation_v1()

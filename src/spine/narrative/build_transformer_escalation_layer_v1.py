from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd
import numpy as np


def build_transformer_escalation_layer_v1():
    root = Path.cwd()
    src = root / "data" / "narrative" / "narrative_diffusion_engine_v1.parquet"
    out = root / "data" / "narrative"
    out.mkdir(parents=True, exist_ok=True)

    if not src.exists():
        raise FileNotFoundError(f"Missing narrative diffusion engine: {src}")

    df = pd.read_parquet(src).copy()

    df["transformer_inference_status"] = "not_enabled_governed_scaffold"
    df["model_family"] = "none"
    df["semantic_override_allowed"] = False

    df["transformer_escalation_pressure"] = df["diffusion_weight"].fillna(0).round(4)

    df["transformer_escalation_state"] = np.select(
        [
            df["transformer_escalation_pressure"] >= 0.70,
            df["transformer_escalation_pressure"] >= 0.55,
            df["transformer_escalation_pressure"] >= 0.40,
            df["transformer_escalation_pressure"] >= 0.25,
        ],
        [
            "systemic_transformer_escalation",
            "fragile_transformer_escalation",
            "elevated_transformer_escalation",
            "watch_transformer_escalation",
        ],
        default="stable_transformer_escalation",
    )

    summary = {
        "component": "transformer_escalation_layer_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "row_count": int(len(df)),
        "inference_enabled": False,
        "semantic_override_allowed": False,
        "status": "transformer_escalation_layer_complete",
    }

    df.to_parquet(out / "transformer_escalation_layer_v1.parquet", index=False)
    df.to_json(out / "transformer_escalation_layer_v1.json", orient="records", indent=2)

    with open(out / "transformer_escalation_layer_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Transformer Escalation Layer complete")
    print("Inference enabled:", summary["inference_enabled"])


if __name__ == "__main__":
    build_transformer_escalation_layer_v1()

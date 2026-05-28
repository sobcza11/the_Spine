from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd
import numpy as np


def build_transition_state_conditioning_layer_v1():
    root = Path.cwd()
    src = root / "data" / "propagation" / "escalation_synchronization_engine_v1.parquet"
    out = root / "data" / "propagation"
    out.mkdir(parents=True, exist_ok=True)

    if not src.exists():
        raise FileNotFoundError(f"Missing escalation sync: {src}")

    df = pd.read_parquet(src).copy()

    df["transition_condition"] = np.select(
        [
            df["recursive_pressure"] >= 0.70,
            df["recursive_pressure"] >= 0.55,
            df["recursive_pressure"] >= 0.40,
            df["recursive_pressure"] >= 0.25,
        ],
        [
            "systemic_transition_condition",
            "fragile_transition_condition",
            "elevated_transition_condition",
            "watch_transition_condition",
        ],
        default="stable_transition_condition",
    )

    df["conditioning_weight"] = np.select(
        [
            df["recursive_pressure"] >= 0.70,
            df["recursive_pressure"] >= 0.55,
            df["recursive_pressure"] >= 0.40,
            df["recursive_pressure"] >= 0.25,
        ],
        [1.00, 0.85, 0.70, 0.50],
        default=0.25,
    )

    summary = {
        "component": "transition_state_conditioning_layer_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "condition_count": int(len(df)),
        "average_conditioning_weight": round(float(df["conditioning_weight"].mean()), 4) if not df.empty else None,
        "status": "transition_state_conditioning_layer_complete",
    }

    df.to_parquet(out / "transition_state_conditioning_layer_v1.parquet", index=False)
    df.to_json(out / "transition_state_conditioning_layer_v1.json", orient="records", indent=2)

    with open(out / "transition_state_conditioning_layer_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Transition-State Conditioning Layer complete")
    print("Conditions:", summary["condition_count"])
    print("Avg weight:", summary["average_conditioning_weight"])


if __name__ == "__main__":
    build_transition_state_conditioning_layer_v1()

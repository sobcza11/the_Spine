from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd
import numpy as np


def build_i2_corporate_fragility_propagation_v1():
    root = Path.cwd()
    src = root / "data" / "i2" / "i2_deterioration_trajectory_engine_v1.parquet"
    out = root / "data" / "i2"
    out.mkdir(parents=True, exist_ok=True)

    if not src.exists():
        raise FileNotFoundError(f"Missing trajectory output: {src}")

    df = pd.read_parquet(src).copy()

    df["i2_fragility_pressure"] = (
        (1 - df["i2_score"]) * 0.60 +
        df["i2_deterioration_pressure"].fillna(0) * 0.40
    ).round(4)

    df["i2_escalation_state"] = np.select(
        [
            df["i2_fragility_pressure"] >= 0.70,
            df["i2_fragility_pressure"] >= 0.55,
            df["i2_fragility_pressure"] >= 0.40,
            df["i2_fragility_pressure"] >= 0.25,
        ],
        [
            "systemic_corporate_fragility",
            "fragile_corporate_deterioration",
            "elevated_corporate_watch",
            "watch_corporate_fragility",
        ],
        default="stable_corporate_durability",
    )

    summary = {
        "component": "i2_corporate_fragility_propagation_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "row_count": int(len(df)),
        "symbol_count": int(df["symbol"].nunique()),
        "average_fragility_pressure": round(float(df["i2_fragility_pressure"].mean()), 4),
        "status": "i2_corporate_fragility_propagation_complete",
    }

    df.to_parquet(out / "i2_corporate_fragility_propagation_v1.parquet", index=False)
    df.to_json(out / "i2_corporate_fragility_propagation_v1.json", orient="records", indent=2)

    with open(out / "i2_corporate_fragility_propagation_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("I2 Corporate Fragility Propagation complete")
    print("Average fragility:", summary["average_fragility_pressure"])


if __name__ == "__main__":
    build_i2_corporate_fragility_propagation_v1()


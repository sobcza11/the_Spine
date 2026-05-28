from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd
import numpy as np


def build_i2_deterioration_trajectory_engine_v1():
    root = Path.cwd()
    src = root / "data" / "i2" / "i2_scoring_engine_v1.parquet"
    out = root / "data" / "i2"
    out.mkdir(parents=True, exist_ok=True)

    if not src.exists():
        raise FileNotFoundError(f"Missing I2 scoring output: {src}")

    df = pd.read_parquet(src).copy()
    df = df.sort_values(["symbol", "year"]).reset_index(drop=True)

    df["i2_score_change_1y"] = df.groupby("symbol")["i2_score"].diff()
    df["i2_score_trend_3y"] = (
        df.groupby("symbol")["i2_score"]
        .rolling(3, min_periods=2)
        .mean()
        .reset_index(level=0, drop=True)
    )

    df["i2_deterioration_pressure"] = (-df["i2_score_change_1y"]).clip(lower=0).fillna(0).round(4)

    df["i2_trajectory_state"] = np.select(
        [
            df["i2_score_change_1y"] >= 0.08,
            df["i2_score_change_1y"] <= -0.08,
            df["i2_score"] >= 0.65,
            df["i2_score"] <= 0.35,
        ],
        [
            "improving_durability",
            "deteriorating_durability",
            "stable_high_durability",
            "persistent_fragility",
        ],
        default="stable_watch",
    )

    summary = {
        "component": "i2_deterioration_trajectory_engine_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "row_count": int(len(df)),
        "symbols_with_history": int(df[df["i2_score_change_1y"].notna()]["symbol"].nunique()),
        "average_deterioration_pressure": round(float(df["i2_deterioration_pressure"].mean()), 4),
        "status": "i2_deterioration_trajectory_engine_complete",
    }

    df.to_parquet(out / "i2_deterioration_trajectory_engine_v1.parquet", index=False)
    df.to_json(out / "i2_deterioration_trajectory_engine_v1.json", orient="records", indent=2)

    with open(out / "i2_deterioration_trajectory_engine_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("I2 Deterioration Trajectory Engine complete")
    print("Avg deterioration:", summary["average_deterioration_pressure"])


if __name__ == "__main__":
    build_i2_deterioration_trajectory_engine_v1()


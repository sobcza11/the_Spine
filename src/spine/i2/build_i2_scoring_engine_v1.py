from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd
import numpy as np


IV_WEIGHTS = {
    "P": 0.16,
    "F": 0.14,
    "L": 0.14,
    "D": 0.14,
    "M": 0.14,
    "X": 0.10,
    "C": 0.10,
    "S": 0.08,
}


def percentile(s, higher=True):
    x = pd.to_numeric(s, errors="coerce")
    r = x.rank(pct=True)
    return r if higher else 1 - r


def build_i2_scoring_engine_v1():
    root = Path.cwd()
    src = root / "data" / "i2" / "i2_recursive_durability_engine_v1.parquet"
    out = root / "data" / "i2"
    out.mkdir(parents=True, exist_ok=True)

    if not src.exists():
        raise FileNotFoundError(f"Missing durability engine output: {src}")

    df = pd.read_parquet(src).copy()

    df["P"] = percentile(df.get("gross_margin", df["i2_recursive_durability_score"]), True).fillna(0.5)
    df["F"] = percentile(df.get("roa", df["i2_recursive_durability_score"]), True).fillna(0.5)
    df["L"] = percentile(df.get("current_ratio", df["i2_recursive_durability_score"]), True).fillna(0.5)
    df["D"] = percentile(df.get("revenue_growth", df["i2_recursive_durability_score"]), True).fillna(0.5)
    df["M"] = percentile(df.get("operating_margin", df["i2_recursive_durability_score"]), True).fillna(0.5)
    df["X"] = 1 - df["i2_recursive_durability_score"]
    df["C"] = percentile(df.get("debt_to_equity", df["i2_recursive_durability_score"]), False).fillna(0.5)
    df["S"] = df["i2_recursive_durability_score"]

    df["i2_score"] = sum(df[k] * w for k, w in IV_WEIGHTS.items()).round(4)

    df["i2_state"] = np.select(
        [
            df["i2_score"] >= 0.80,
            df["i2_score"] >= 0.65,
            df["i2_score"] >= 0.50,
            df["i2_score"] >= 0.35,
        ],
        [
            "graham_aligned_compounder",
            "financially_durable",
            "neutral_watch",
            "fragility_watch",
        ],
        default="structural_impairment",
    )

    summary = {
        "component": "i2_scoring_engine_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "row_count": int(len(df)),
        "symbol_count": int(df["symbol"].nunique()),
        "average_i2_score": round(float(df["i2_score"].mean()), 4),
        "iv_vector": list(IV_WEIGHTS.keys()),
        "status": "i2_scoring_engine_complete",
    }

    df.to_parquet(out / "i2_scoring_engine_v1.parquet", index=False)
    df.to_json(out / "i2_scoring_engine_v1.json", orient="records", indent=2)

    with open(out / "i2_scoring_engine_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("I2 Scoring Engine complete")
    print("Average I2 Score:", summary["average_i2_score"])


if __name__ == "__main__":
    build_i2_scoring_engine_v1()


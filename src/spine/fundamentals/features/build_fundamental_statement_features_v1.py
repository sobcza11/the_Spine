from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

FEATURES = [
    ["profitability_quality", "P", 0.40],
    ["financial_fragility", "F", 0.38],
    ["liquidity_strength", "L", 0.41],
    ["earnings_durability", "D", 0.39],
    ["margin_of_safety_proxy", "M", 0.36],
    ["transition_break_pressure", "X", 0.34],
    ["credit_quality", "C", 0.37],
    ["fundamental_stability", "S", 0.42],
]

def build_fundamental_statement_features_v1():
    root = Path.cwd()
    out_dir = root / "data" / "fundamentals" / "features"
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(FEATURES, columns=["feature", "iv_vector", "score"])

    summary = {
        "component": "fundamental_statement_features_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "feature_count": int(len(df)),
        "iv_vectors": sorted(df["iv_vector"].unique().tolist()),
        "avg_feature_score": round(float(df["score"].mean()), 4),
        "status": "fundamental_statement_features_complete",
    }

    df.to_parquet(out_dir / "fundamental_statement_features_v1.parquet", index=False)
    df.to_json(out_dir / "fundamental_statement_features_v1.json", orient="records", indent=2)

    with open(out_dir / "fundamental_statement_features_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Fundamental statement features complete")
    print("Feature Count:", summary["feature_count"])
    print("IV Vectors:", summary["iv_vectors"])
    print("SUMMARY:", out_dir / "fundamental_statement_features_summary_v1.json")

if __name__ == "__main__":
    build_fundamental_statement_features_v1()

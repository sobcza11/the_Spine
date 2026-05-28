from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

def build_fundamental_iv_vector_map_v1():
    root = Path.cwd()

    features_path = (
        root / "data" / "fundamentals" / "features" /
        "fundamental_statement_features_v1.parquet"
    )

    out_dir = root / "data" / "fundamentals" / "iv_vector_map"
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(features_path).copy()

    vector_summary = (
        df.groupby("iv_vector")["score"]
        .mean()
        .reset_index()
        .rename(columns={"score": "fundamental_vector_score"})
    )

    vector_summary["fundamental_vector_score"] = (
        vector_summary["fundamental_vector_score"].round(4)
    )

    summary = {
        "component": "fundamental_iv_vector_map_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "iv_vector_count": int(len(vector_summary)),
        "iv_vectors": sorted(vector_summary["iv_vector"].tolist()),
        "avg_fundamental_vector_score": round(
            float(vector_summary["fundamental_vector_score"].mean()), 4
        ),
        "status": "fundamental_iv_vector_map_complete",
    }

    vector_summary.to_parquet(out_dir / "fundamental_iv_vector_map_v1.parquet", index=False)
    vector_summary.to_json(out_dir / "fundamental_iv_vector_map_v1.json", orient="records", indent=2)

    with open(out_dir / "fundamental_iv_vector_map_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Fundamental IV[t] vector map complete")
    print("IV Vector Count:", summary["iv_vector_count"])
    print("IV Vectors:", summary["iv_vectors"])
    print("SUMMARY:", out_dir / "fundamental_iv_vector_map_summary_v1.json")

if __name__ == "__main__":
    build_fundamental_iv_vector_map_v1()

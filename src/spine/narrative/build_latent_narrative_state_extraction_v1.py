from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd
import numpy as np


def build_latent_narrative_state_extraction_v1():
    root = Path.cwd()
    src = root / "data" / "narrative" / "semantic_pressure_scoring_v1.parquet"
    out = root / "data" / "narrative"
    out.mkdir(parents=True, exist_ok=True)

    if not src.exists():
        raise FileNotFoundError(f"Missing semantic pressure scoring: {src}")

    df = pd.read_parquet(src).copy()

    df["latent_narrative_state"] = np.select(
        [
            df["semantic_pressure"] >= 0.70,
            df["semantic_pressure"] >= 0.55,
            df["semantic_pressure"] >= 0.40,
            df["semantic_pressure"] >= 0.25,
        ],
        [
            "latent_contraction_narrative",
            "latent_fragility_narrative",
            "latent_mixed_pressure_narrative",
            "latent_watch_narrative",
        ],
        default="latent_stable_narrative",
    )

    df["latent_narrative_confidence"] = (
        (df["stress_term_count"] + df["strength_term_count"])
        .clip(lower=0, upper=20) / 20
    ).round(4)

    summary = {
        "component": "latent_narrative_state_extraction_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "row_count": int(len(df)),
        "average_confidence": round(float(df["latent_narrative_confidence"].mean()), 4),
        "status": "latent_narrative_state_extraction_complete",
    }

    df.to_parquet(out / "latent_narrative_state_extraction_v1.parquet", index=False)
    df.to_json(out / "latent_narrative_state_extraction_v1.json", orient="records", indent=2)

    with open(out / "latent_narrative_state_extraction_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Latent Narrative-State Extraction complete")
    print("Average confidence:", summary["average_confidence"])


if __name__ == "__main__":
    build_latent_narrative_state_extraction_v1()

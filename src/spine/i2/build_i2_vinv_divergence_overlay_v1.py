from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd
import numpy as np


def build_i2_vinv_divergence_overlay_v1():
    root = Path.cwd()
    src = root / "data" / "i2" / "i2_scoring_engine_v1.parquet"
    out = root / "data" / "i2"
    out.mkdir(parents=True, exist_ok=True)

    if not src.exists():
        raise FileNotFoundError(f"Missing I2 scoring output: {src}")

    df = pd.read_parquet(src).copy()

    vinv_candidates = list(root.glob("data/**/*vinv*summary*.json"))
    vinv_context = "vinv_context_available" if vinv_candidates else "vinv_context_missing"

    df["vinv_proxy_score"] = 0.50
    df["i2_vinv_spread"] = (df["i2_score"] - df["vinv_proxy_score"]).round(4)

    df["i2_vinv_divergence_state"] = np.select(
        [
            df["i2_vinv_spread"] >= 0.20,
            df["i2_vinv_spread"] <= -0.20,
            df["i2_score"] >= 0.65,
        ],
        [
            "underappreciated_durability",
            "speculative_reflexivity_risk",
            "durable_aligned",
        ],
        default="neutral_alignment",
    )

    summary = {
        "component": "i2_vinv_divergence_overlay_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "row_count": int(len(df)),
        "vinv_context": vinv_context,
        "average_spread": round(float(df["i2_vinv_spread"].mean()), 4),
        "status": "i2_vinv_divergence_overlay_complete",
    }

    df.to_parquet(out / "i2_vinv_divergence_overlay_v1.parquet", index=False)
    df.to_json(out / "i2_vinv_divergence_overlay_v1.json", orient="records", indent=2)

    with open(out / "i2_vinv_divergence_overlay_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("I2 / VinV Divergence Overlay complete")
    print("VinV Context:", vinv_context)


if __name__ == "__main__":
    build_i2_vinv_divergence_overlay_v1()


from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


TARGETS = ["GeoScen", "IV[t]", "FinState", "COT", "RATES", "VinV", "I2"]


def build_narrative_diffusion_engine_v1():
    root = Path.cwd()
    src = root / "data" / "narrative" / "latent_narrative_state_extraction_v1.parquet"
    out = root / "data" / "narrative"
    out.mkdir(parents=True, exist_ok=True)

    if not src.exists():
        raise FileNotFoundError(f"Missing latent narrative state extraction: {src}")

    df = pd.read_parquet(src).copy()

    rows = []
    for _, r in df.iterrows():
        for target in TARGETS:
            rows.append({
                "source_file": r["source_file"],
                "target_engine": target,
                "latent_narrative_state": r["latent_narrative_state"],
                "semantic_pressure": r["semantic_pressure"],
                "latent_narrative_confidence": r["latent_narrative_confidence"],
                "diffusion_weight": round(float(r["semantic_pressure"]) * float(r["latent_narrative_confidence"]), 4),
            })

    diffusion = pd.DataFrame(rows)

    summary = {
        "component": "narrative_diffusion_engine_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "edge_count": int(len(diffusion)),
        "target_count": int(diffusion["target_engine"].nunique()) if not diffusion.empty else 0,
        "average_diffusion_weight": round(float(diffusion["diffusion_weight"].mean()), 4) if not diffusion.empty else None,
        "status": "narrative_diffusion_engine_complete",
    }

    diffusion.to_parquet(out / "narrative_diffusion_engine_v1.parquet", index=False)
    diffusion.to_json(out / "narrative_diffusion_engine_v1.json", orient="records", indent=2)

    with open(out / "narrative_diffusion_engine_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Narrative Diffusion Engine complete")
    print("Edges:", summary["edge_count"])
    print("Targets:", summary["target_count"])


if __name__ == "__main__":
    build_narrative_diffusion_engine_v1()

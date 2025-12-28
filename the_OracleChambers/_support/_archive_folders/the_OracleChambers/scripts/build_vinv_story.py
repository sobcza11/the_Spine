# scripts/build_vinv_story.py

from pathlib import Path
import json

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def load_paths_config() -> dict:
    config_path = ROOT / "config" / "artifacts_paths.json"
    with config_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_parquet_from_key(artifact_key: str) -> pd.DataFrame:
    paths = load_paths_config()
    if artifact_key not in paths:
        raise KeyError(f"Artifact key '{artifact_key}' not in artifacts_paths.json")

    parquet_path = ROOT / paths[artifact_key]
    if not parquet_path.exists():
        raise FileNotFoundError(f"Artifact '{artifact_key}' not found at {parquet_path}")

    return pd.read_parquet(parquet_path)


def classify_vinv_regime(score: float) -> str:
    """
    Simple regime mapping:
    - vinv_score >= 0.33  → in_vogue
    - vinv_score <= -0.33 → out_of_favor
    - else                → transition
    """
    if np.isnan(score):
        return "unknown"
    if score >= 0.33:
        return "in_vogue"
    if score <= -0.33:
        return "out_of_favor"
    return "transition"


def build_vinv_story(vinv_df: pd.DataFrame) -> pd.DataFrame:
    df = vinv_df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    df["vinv_regime"] = df["vinv_score"].apply(classify_vinv_regime)

    # Very simple text block (OracleChambers will later enrich this)
    def _make_blurb(row) -> str:
        regime = row["vinv_regime"]
        score = row["vinv_score"]
        if regime == "in_vogue":
            return f"Value leadership is structurally supported (VinV={score:.2f})."
        if regime == "out_of_favor":
            return f"Value remains under pressure relative to growth (VinV={score:.2f})."
        if regime == "transition":
            return f"Style leadership is rotating; VinV suggests a transitional regime (VinV={score:.2f})."
        return "VinV regime is currently unknown."

    df["vinv_story"] = df.apply(_make_blurb, axis=1)

    out = df[["date", "vinv_regime", "vinv_story"]].copy()
    return out


def main() -> None:
    paths = load_paths_config()

    vinv_df = load_parquet_from_key("vinv_signal")

    story_df = build_vinv_story(vinv_df)

    # Where to write narratives
    if "vinv_story" not in paths:
        # You can either add this to artifacts_paths.json
        # or default to a standard narratives location:
        narratives_path = ROOT / "data" / "processed" / "narratives" / "vinv_story.parquet"
    else:
        narratives_path = ROOT / paths["vinv_story"]

    narratives_path.parent.mkdir(parents=True, exist_ok=True)
    story_df.to_parquet(narratives_path, index=False)

    print(f"Wrote vinv_story → {narratives_path}")


if __name__ == "__main__":
    main()


from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


STRESS_TERMS = [
    "contraction",
    "decline",
    "weak",
    "slowing",
    "pressure",
    "inflation",
    "backlog",
    "shortage",
    "uncertain",
    "soft",
    "deterioration",
    "risk",
]

STRENGTH_TERMS = [
    "expansion",
    "growth",
    "strong",
    "improving",
    "resilient",
    "demand",
    "orders",
    "stable",
    "recovery",
    "positive",
]


def score_text(text: str):
    t = str(text).lower()
    stress = sum(t.count(w) for w in STRESS_TERMS)
    strength = sum(t.count(w) for w in STRENGTH_TERMS)
    total = stress + strength

    if total == 0:
        return 0.0, stress, strength

    pressure = stress / total
    return round(float(pressure), 4), stress, strength


def classify(x):
    if x >= 0.70: return "systemic_semantic_pressure"
    if x >= 0.55: return "fragile_semantic_pressure"
    if x >= 0.40: return "elevated_semantic_pressure"
    if x >= 0.25: return "watch_semantic_pressure"
    return "stable_semantic_pressure"


def build_semantic_pressure_scoring_v1():
    root = Path.cwd()
    src = root / "data" / "narrative" / "ism_narrative_ingestion_v1.parquet"
    out = root / "data" / "narrative"
    out.mkdir(parents=True, exist_ok=True)

    if not src.exists():
        raise FileNotFoundError(f"Missing ISM narrative ingestion: {src}")

    df = pd.read_parquet(src).copy()

    scores = df["text_sample"].apply(score_text)
    df["semantic_pressure"] = [s[0] for s in scores]
    df["stress_term_count"] = [s[1] for s in scores]
    df["strength_term_count"] = [s[2] for s in scores]
    df["semantic_pressure_state"] = df["semantic_pressure"].apply(classify)

    summary = {
        "component": "semantic_pressure_scoring_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "row_count": int(len(df)),
        "average_semantic_pressure": round(float(df["semantic_pressure"].mean()), 4),
        "status": "semantic_pressure_scoring_complete",
    }

    df.to_parquet(out / "semantic_pressure_scoring_v1.parquet", index=False)
    df.to_json(out / "semantic_pressure_scoring_v1.json", orient="records", indent=2)

    with open(out / "semantic_pressure_scoring_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Semantic Pressure Scoring complete")
    print("Average pressure:", summary["average_semantic_pressure"])


if __name__ == "__main__":
    build_semantic_pressure_scoring_v1()

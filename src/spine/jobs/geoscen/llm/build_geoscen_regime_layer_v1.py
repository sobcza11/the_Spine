from pathlib import Path
import pandas as pd


REPO_ROOT = Path.cwd()

INPUT_PATH = REPO_ROOT / "data/geoscen/llm/geoscen_rbl_template_v2.parquet"
OUTPUT_PATH = REPO_ROOT / "data/geoscen/llm/geoscen_regime_layer_v1.parquet"


def classify_regime(row):
    tone = row["tone_direction"]
    dominance = row["dominance_mean"]
    strength = row["signal_strength"]

    if tone <= -0.50 and dominance >= 0.30:
        return "Risk-Off / Tightening Pressure"

    if tone >= 0.50 and dominance >= 0.30:
        return "Risk-On / Reflation Pressure"

    if strength >= 0.30 and abs(tone) < 0.25:
        return "High-Signal / Mixed Regime"

    if dominance < 0.20:
        return "Low-Conviction / Monitoring Regime"

    return "Transitional / Mixed Macro Regime"


def main():
    df = pd.read_parquet(INPUT_PATH).copy()

    df["regime_label"] = df.apply(classify_regime, axis=1)
    df["regime_confidence"] = (
        pd.to_numeric(df["dominance_mean"], errors="coerce").fillna(0)
        + pd.to_numeric(df["signal_strength"], errors="coerce").fillna(0)
        + pd.to_numeric(df["tone_direction"], errors="coerce").fillna(0).abs()
    ) / 3

    df["regime_summary"] = (
        df["regime_label"]
        + " | confidence="
        + df["regime_confidence"].round(3).astype(str)
    )

    df["version"] = "geoscen_regime_layer_v1"

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUTPUT_PATH, index=False)

    print("OK | GeoScen regime layer v1")
    print("output:", OUTPUT_PATH)
    print(df[["date", "regime_label", "regime_confidence"]])


if __name__ == "__main__":
    main()
    
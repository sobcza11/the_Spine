from pathlib import Path
import pandas as pd


REPO_ROOT = Path.cwd()

CONTEXT_PATH = REPO_ROOT / "data/geoscen/llm/geoscen_context_pack_v1.parquet"
FINBERT_PATH = REPO_ROOT / "data/geoscen/llm/geoscen_finbert_tone_pack_v1.parquet"
NUANCE_PATH = REPO_ROOT / "data/geoscen/llm/geoscen_finbert_tone_nuance_pack_v1.parquet"

OUTPUT_PATH = REPO_ROOT / "data/geoscen/llm/geoscen_context_pack_tone_v1.parquet"


def summarize_tone(df: pd.DataFrame, label_col: str, weighted_col: str, prefix: str) -> pd.DataFrame:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    grouped = (
        df.groupby("date", dropna=False)
        .agg(
            avg_weighted_tone=(weighted_col, "mean"),
            positive_count=(label_col, lambda x: (x.astype(str).str.lower() == "positive").sum()),
            neutral_count=(label_col, lambda x: (x.astype(str).str.lower() == "neutral").sum()),
            negative_count=(label_col, lambda x: (x.astype(str).str.lower() == "negative").sum()),
            total_count=(label_col, "count"),
        )
        .reset_index()
    )

    grouped = grouped.rename(
        columns={
            "avg_weighted_tone": f"{prefix}_avg_weighted_tone",
            "positive_count": f"{prefix}_positive_count",
            "neutral_count": f"{prefix}_neutral_count",
            "negative_count": f"{prefix}_negative_count",
            "total_count": f"{prefix}_total_count",
        }
    )

    return grouped


def main():
    context = pd.read_parquet(CONTEXT_PATH).copy()
    finbert = pd.read_parquet(FINBERT_PATH).copy()
    nuance = pd.read_parquet(NUANCE_PATH).copy()

    context["date"] = pd.to_datetime(context["date"], errors="coerce")

    finbert_summary = summarize_tone(
        finbert,
        label_col="finbert_label",
        weighted_col="finbert_weighted_tone",
        prefix="finbert",
    )

    nuance_summary = summarize_tone(
        nuance,
        label_col="finbert_tone_label",
        weighted_col="finbert_tone_weighted_value",
        prefix="finbert_tone",
    )

    out = context.merge(finbert_summary, on="date", how="left")
    out = out.merge(nuance_summary, on="date", how="left")

    tone_cols = [c for c in out.columns if c.startswith("finbert")]
    for c in tone_cols:
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0)

    out["tone_context"] = (
        "[Tone Layer]\n"
        + "- FinBERT avg weighted tone: "
        + out["finbert_avg_weighted_tone"].round(4).astype(str)
        + " | positive="
        + out["finbert_positive_count"].astype(int).astype(str)
        + ", neutral="
        + out["finbert_neutral_count"].astype(int).astype(str)
        + ", negative="
        + out["finbert_negative_count"].astype(int).astype(str)
        + "\n- finbert-tone avg weighted tone: "
        + out["finbert_tone_avg_weighted_tone"].round(4).astype(str)
        + " | positive="
        + out["finbert_tone_positive_count"].astype(int).astype(str)
        + ", neutral="
        + out["finbert_tone_neutral_count"].astype(int).astype(str)
        + ", negative="
        + out["finbert_tone_negative_count"].astype(int).astype(str)
    )

    out["full_context_tone"] = out["full_context"].fillna("").astype(str) + "\n\n" + out["tone_context"]

    out["version"] = "geoscen_context_pack_tone_v1"

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUTPUT_PATH, index=False)

    print("OK | GeoScen context pack tone v1")
    print("output:", OUTPUT_PATH)
    print("shape:", out.shape)
    print(out.tail(5)[["date", "finbert_avg_weighted_tone", "finbert_tone_avg_weighted_tone", "version"]])


if __name__ == "__main__":
    main()
    
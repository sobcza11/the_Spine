from pathlib import Path
import pandas as pd
import torch
from transformers import pipeline


REPO_ROOT = Path.cwd()

INPUT_PATH = REPO_ROOT / "data/geoscen/llm/geoscen_llm_input_v1.parquet"
OUTPUT_PATH = REPO_ROOT / "data/geoscen/llm/geoscen_finbert_tone_pack_v1.parquet"

MODEL_NAME = "ProsusAI/finbert"


def main():
    df = pd.read_parquet(INPUT_PATH).copy()

    df["tone_text"] = (
        df["text_excerpt"].fillna("").astype(str)
        .where(df["text_excerpt"].fillna("").astype(str).str.len() > 30, df["llm_context"].fillna("").astype(str))
        .str[:1200]
    )

    df = df[df["tone_text"].str.len() > 30].copy()

    device = 0 if torch.cuda.is_available() else -1

    clf = pipeline(
        "text-classification",
        model=MODEL_NAME,
        tokenizer=MODEL_NAME,
        device=device,
        truncation=True,
        max_length=512,
    )

    results = clf(df["tone_text"].tolist(), batch_size=16)

    df["finbert_label"] = [r["label"] for r in results]
    df["finbert_score"] = [float(r["score"]) for r in results]

    label_map = {
        "positive": 1,
        "neutral": 0,
        "negative": -1,
    }

    df["finbert_tone_value"] = df["finbert_label"].str.lower().map(label_map).fillna(0)
    df["finbert_weighted_tone"] = df["finbert_tone_value"] * df["finbert_score"]

    keep_cols = [
        "date",
        "source_family",
        "source_name",
        "zt_module",
        "sector",
        "bank",
        "document_id",
        "primary_tag",
        "finbert_label",
        "finbert_score",
        "finbert_tone_value",
        "finbert_weighted_tone",
        "tone_text",
        "row_sha256",
    ]

    out = df[keep_cols].sort_values(["date", "source_family", "source_name"]).reset_index(drop=True)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUTPUT_PATH, index=False)

    print("OK | GeoScen FinBERT tone pack v1")
    print("output:", OUTPUT_PATH)
    print("shape:", out.shape)
    print(out["finbert_label"].value_counts(dropna=False))
    print(out.tail(10)[["date", "source_family", "finbert_label", "finbert_score", "finbert_weighted_tone"]])


if __name__ == "__main__":
    main()
    
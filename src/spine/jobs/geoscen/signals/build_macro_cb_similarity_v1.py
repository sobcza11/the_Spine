import os
import numpy as np
import pandas as pd

INPUT_PATH = "data/geoscen/signals/macro_cb_embeddings_v1.parquet"
OUTPUT_PATH = "data/geoscen/signals/macro_cb_similarity_v1.parquet"

TOP_N = 10


def to_vector(x):
    return np.array(x, dtype="float32")


def run():
    df = pd.read_parquet(INPUT_PATH).copy()

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["embedding_vector"].notna()].copy()

    vectors = np.vstack(df["embedding_vector"].apply(to_vector).to_list())

    rows = []

    for i, row in df.iterrows():
        sims = vectors @ vectors[i]

        candidates = df.copy()
        candidates["similarity_score"] = sims

        candidates = candidates[candidates.index != i]
        candidates = candidates.sort_values("similarity_score", ascending=False).head(TOP_N)

        for _, match in candidates.iterrows():
            rows.append({
                "bank_code": row["bank_code"],
                "date": row["date"].date().isoformat(),
                "document_type": row["document_type"],
                "title": row["title"],
                "url": row["url"],
                "match_bank_code": match["bank_code"],
                "match_date": match["date"].date().isoformat(),
                "match_document_type": match["document_type"],
                "match_title": match["title"],
                "match_url": match["url"],
                "similarity_score": float(match["similarity_score"]),
                "comparison_type": (
                    "cross_bank" if row["bank_code"] != match["bank_code"] else "within_bank"
                ),
                "embedding_model": row["embedding_model"],
                "source_layer": "macro_cb_similarity_v1",
                "version": "v1",
            })

    out = pd.DataFrame(rows)

    out = out.sort_values(
        ["bank_code", "date", "similarity_score"],
        ascending=[True, True, False],
    ).reset_index(drop=True)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    out.to_parquet(OUTPUT_PATH, index=False)

    print("macro_cb similarity rows:", len(out))


if __name__ == "__main__":
    run()


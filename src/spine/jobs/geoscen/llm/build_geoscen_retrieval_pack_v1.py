from pathlib import Path
import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer


REPO_ROOT = Path.cwd()

INPUT_PATH = REPO_ROOT / "data/geoscen/llm/geoscen_llm_input_v1.parquet"
OUTPUT_PATH = REPO_ROOT / "data/geoscen/llm/geoscen_retrieval_pack_v1.parquet"

MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"


def norm_abs(series):
    x = pd.to_numeric(series, errors="coerce").fillna(0).abs()
    return x / x.max() if x.max() != 0 else x


def main():
    df = pd.read_parquet(INPUT_PATH).copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Build retrieval text
    df["retrieval_text"] = (
        df["source_family"].astype(str)
        + " "
        + df["zt_module"].astype(str)
        + " "
        + df["primary_tag"].astype(str)
        + " "
        + df["llm_context"].astype(str)
    )

    print("Loading MiniLM model...")
    model = SentenceTransformer(MODEL_NAME)

    query = "macro conditions policy tone PMI inflation growth risk labor financial conditions"
    query_vec = model.encode([query], normalize_embeddings=True)[0]

    print("Encoding text...")
    text_vecs = model.encode(
        df["retrieval_text"].tolist(),
        normalize_embeddings=True,
        show_progress_bar=True
    )

    df["embedding_similarity"] = np.dot(text_vecs, query_vec)

    df["signal_strength"] = norm_abs(df["numeric_signal_value"])
    df["confidence_norm"] = pd.to_numeric(df["confidence"], errors="coerce").fillna(0)

    df["retrieval_score"] = (
        0.5 * df["embedding_similarity"]
        + 0.3 * df["signal_strength"]
        + 0.2 * df["confidence_norm"]
    )

    out = df.sort_values(["date", "retrieval_score"], ascending=[True, False])

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUTPUT_PATH, index=False)

    print("OK | GeoScen retrieval pack v1")
    print("output:", OUTPUT_PATH)
    print("shape:", out.shape)
    print(out.tail(10)[["date", "source_family", "retrieval_score"]])


if __name__ == "__main__":
    main()


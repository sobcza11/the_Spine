import os
import hashlib
import pandas as pd
from sentence_transformers import SentenceTransformer

INPUT_PATH = "data/geoscen/cb/macro_cb_canonical_v1.parquet"
OUTPUT_PATH = "data/geoscen/signals/macro_cb_embeddings_v1.parquet"

MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"


def sha256_text(text):
    return hashlib.sha256(str(text).encode("utf-8")).hexdigest()


def run():
    df = pd.read_parquet(INPUT_PATH).copy()

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["text"] = df["text"].fillna("").astype(str)
    df = df[df["text"].str.len() > 0].copy()

    model = SentenceTransformer(MODEL_NAME)

    embeddings = model.encode(
        df["text"].tolist(),
        show_progress_bar=True,
        normalize_embeddings=True,
    )

    out = df[
        [
            "bank",
            "bank_code",
            "currency",
            "date",
            "document_type",
            "title",
            "url",
            "text_chars",
        ]
    ].copy()

    out["text_sha256"] = df["text"].apply(sha256_text)
    out["embedding_model"] = MODEL_NAME
    out["embedding_vector"] = embeddings.tolist()
    out["source_layer"] = "macro_cb_embeddings_v1"
    out["version"] = "v1"

    out["date"] = out["date"].dt.date.astype("string")

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    out.to_parquet(OUTPUT_PATH, index=False)

    print("macro_cb embeddings rows:", len(out))


if __name__ == "__main__":
    run()


import pandas as pd

OUTPUT_PATH = "data/geoscen/signals/macro_cb_similarity_v1.parquet"

REQUIRED_COLS = [
    "bank_code",
    "date",
    "document_type",
    "title",
    "url",
    "match_bank_code",
    "match_date",
    "match_document_type",
    "match_title",
    "match_url",
    "similarity_score",
    "comparison_type",
    "embedding_model",
    "source_layer",
    "version",
]


def run():
    df = pd.read_parquet(OUTPUT_PATH)

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    if len(df) < 2000:
        raise ValueError(f"Row count too low: {len(df)}")

    if df["similarity_score"].isna().any():
        raise ValueError("Missing similarity scores detected")

    if not df["similarity_score"].between(-1, 1).all():
        raise ValueError("Similarity score outside expected cosine range")

    if df["comparison_type"].isna().any():
        raise ValueError("Missing comparison_type detected")

    print("macro_cb similarity validation passed:", len(df))


if __name__ == "__main__":
    run()


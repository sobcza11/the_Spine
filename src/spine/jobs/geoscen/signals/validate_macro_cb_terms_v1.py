import pandas as pd

OUTPUT_PATH = "data/geoscen/signals/macro_cb_terms_v1.parquet"

REQUIRED_COLS = [
    "bank",
    "bank_code",
    "currency",
    "date",
    "document_type",
    "title",
    "url",
    "term",
    "tfidf_score",
    "term_count",
    "term_rank",
    "source_layer",
    "version",
]


def run():
    df = pd.read_parquet(OUTPUT_PATH)

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    if len(df) < 4000:
        raise ValueError(f"Row count too low: {len(df)}")

    if df["term"].isna().any():
        raise ValueError("Missing terms detected")

    if df["tfidf_score"].isna().any():
        raise ValueError("Missing TF-IDF scores detected")

    print("macro_cb terms validation passed:", len(df))


if __name__ == "__main__":
    run()


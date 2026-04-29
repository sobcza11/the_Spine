import pandas as pd

ECB_OUTPUT_COMBINED_PATH = "data/geoscen/cb/ecb/ecb_combined_canonical_v1.parquet"

REQUIRED_COLS = [
    "bank",
    "bank_code",
    "currency",
    "document_type",
    "title",
    "date",
    "url",
    "text",
    "text_chars",
    "ingested_at_utc",
    "source_layer",
    "version",
]


def run():
    df = pd.read_parquet(ECB_OUTPUT_COMBINED_PATH)

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    if len(df) < 100:
        raise ValueError(f"ECB combined row count too low: {len(df)}")

    if df["text_chars"].min() <= 0:
        raise ValueError("Empty text detected")

    print("ECB combined canonical validation passed:", len(df))


if __name__ == "__main__":
    run()


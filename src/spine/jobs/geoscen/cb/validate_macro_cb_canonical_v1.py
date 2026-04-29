import pandas as pd

OUTPUT_PATH = "data/geoscen/cb/macro_cb_canonical_v1.parquet"

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
]


def run():
    df = pd.read_parquet(OUTPUT_PATH)

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    if len(df) < 100:
        raise ValueError(f"Row count too low: {len(df)}")

    if df["text_chars"].min() <= 0:
        raise ValueError("Empty text detected")

    print("macro_cb canonical validation passed:", len(df))


if __name__ == "__main__":
    run()


import pandas as pd

OUTPUT_PATH = "data/geoscen/signals/macro_cb_signals_v1.parquet"

REQUIRED_COLS = [
    "bank",
    "bank_code",
    "currency",
    "date",
    "document_type",
    "title",
    "url",
    "text_chars",
    "text_word_count",
    "hawkish_count",
    "dovish_count",
    "uncertainty_count",
    "policy_tone_score",
    "uncertainty_score",
    "policy_tone_per_1k_words",
    "uncertainty_per_1k_words",
]


def run():
    df = pd.read_parquet(OUTPUT_PATH)

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    if len(df) < 150:
        raise ValueError(f"Row count too low: {len(df)}")

    if df["date"].isna().any():
        raise ValueError("Missing dates detected")

    print("macro_cb signals validation passed:", len(df))


if __name__ == "__main__":
    run()

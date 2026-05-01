import pandas as pd

OUTPUT_PATH = "data/geoscen/signals/macro_cb_monthly_aggregates_v1.parquet"

REQUIRED_COLS = [
    "bank",
    "bank_code",
    "currency",
    "month",
    "documents",
    "avg_policy_tone_score",
    "avg_uncertainty_score",
    "avg_policy_tone_per_1k_words",
    "avg_uncertainty_per_1k_words",
    "total_hawkish_count",
    "total_dovish_count",
    "total_uncertainty_count",
    "total_text_chars",
    "total_text_word_count",
    "source_layer",
    "version",
]


def run():
    df = pd.read_parquet(OUTPUT_PATH)

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    if len(df) < 100:
        raise ValueError(f"Row count too low: {len(df)}")

    if df["month"].isna().any():
        raise ValueError("Missing month values detected")

    if (df["documents"] <= 0).any():
        raise ValueError("Invalid document count detected")

    print("macro_cb monthly aggregates validation passed:", len(df))


if __name__ == "__main__":
    run()


import os
import pandas as pd

INPUT_PATH = "data/geoscen/signals/macro_cb_signals_v1.parquet"
OUTPUT_PATH = "data/geoscen/signals/macro_cb_monthly_aggregates_v1.parquet"


def run():
    df = pd.read_parquet(INPUT_PATH).copy()

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["month"] = df["date"].dt.to_period("M").astype(str)

    grouped = (
        df.groupby(["bank", "bank_code", "currency", "month"], as_index=False)
        .agg(
            documents=("url", "count"),
            avg_policy_tone_score=("policy_tone_score", "mean"),
            avg_uncertainty_score=("uncertainty_score", "mean"),
            avg_policy_tone_per_1k_words=("policy_tone_per_1k_words", "mean"),
            avg_uncertainty_per_1k_words=("uncertainty_per_1k_words", "mean"),
            total_hawkish_count=("hawkish_count", "sum"),
            total_dovish_count=("dovish_count", "sum"),
            total_uncertainty_count=("uncertainty_count", "sum"),
            total_text_chars=("text_chars", "sum"),
            total_text_word_count=("text_word_count", "sum"),
        )
    )

    grouped["source_layer"] = "macro_cb_monthly_aggregates_v1"
    grouped["version"] = "v1"

    grouped = grouped.sort_values(["bank_code", "month"]).reset_index(drop=True)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    grouped.to_parquet(OUTPUT_PATH, index=False)

    print("macro_cb monthly aggregate rows:", len(grouped))


if __name__ == "__main__":
    run()
    
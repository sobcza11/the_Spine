import pandas as pd

OUTPUT_PATH = "data/geoscen/signals/isovector_macro_cb_view_v1.parquet"

REQUIRED_COLS = [
    "date",
    "bank",
    "bank_code",
    "currency",
    "documents",
    "policy_tone",
    "uncertainty",
    "policy_tone_per_1k_words",
    "uncertainty_per_1k_words",
    "total_hawkish_count",
    "total_dovish_count",
    "total_uncertainty_count",
    "total_text_word_count",
    "source_layer",
    "version",
    "isovector_component",
    "signal_family",
    "signal_frequency",
]


def run():
    df = pd.read_parquet(OUTPUT_PATH)

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    if len(df) < 100:
        raise ValueError(f"Row count too low: {len(df)}")

    if df["date"].isna().any():
        raise ValueError("Missing date values detected")

    if (df["documents"] <= 0).any():
        raise ValueError("Invalid document counts detected")

    print("IsoVector macro_cb view validation passed:", len(df))


if __name__ == "__main__":
    run()


import os
import pandas as pd

INPUT_PATH = "data/geoscen/signals/macro_cb_monthly_aggregates_v1.parquet"
OUTPUT_PATH = "data/geoscen/signals/isovector_macro_cb_view_v1.parquet"


def run():
    df = pd.read_parquet(INPUT_PATH).copy()

    out = df.rename(
        columns={
            "month": "date",
            "avg_policy_tone_score": "policy_tone",
            "avg_uncertainty_score": "uncertainty",
            "avg_policy_tone_per_1k_words": "policy_tone_per_1k_words",
            "avg_uncertainty_per_1k_words": "uncertainty_per_1k_words",
        }
    )

    keep_cols = [
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
    ]

    out = out[keep_cols].sort_values(["bank_code", "date"]).reset_index(drop=True)

    out["isovector_component"] = "GeoScen"
    out["signal_family"] = "macro_cb"
    out["signal_frequency"] = "monthly"

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    out.to_parquet(OUTPUT_PATH, index=False)

    print("IsoVector macro_cb view rows:", len(out))


if __name__ == "__main__":
    run()


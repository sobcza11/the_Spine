import pandas as pd

OUTPUT_PATH = "data/geoscen/signals/macro_cb_oc_signals_v1.parquet"

REQUIRED_COLS = [
    "date",
    "bank",
    "bank_code",
    "currency",
    "documents",
    "policy_tone",
    "uncertainty",
    "it_de_10y_spread",
    "spread_lag1",
    "spread_change_lag1",
    "fragmentation_pressure_flag",
    "hawkish_under_stress_flag",
    "uncertainty_without_spread_flag",
    "calm_policy_flag",
    "tone_diff",
    "policy_divergence_flag",
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

    if df["date"].isna().any():
        raise ValueError("Missing date values detected")

    flag_cols = [
        "fragmentation_pressure_flag",
        "hawkish_under_stress_flag",
        "uncertainty_without_spread_flag",
        "calm_policy_flag",
        "policy_divergence_flag",
    ]

    for col in flag_cols:
        vals = set(df[col].dropna().unique())
        if not vals.issubset({0, 1}):
            raise ValueError(f"Invalid flag values in {col}: {vals}")

    print("macro_cb OC signals validation passed:", len(df))


if __name__ == "__main__":
    run()


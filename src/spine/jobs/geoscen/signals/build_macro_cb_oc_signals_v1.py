import os
import pandas as pd

INPUT_PATH = "data/geoscen/signals/isovector_macro_cb_rates_join_v1.parquet"
OUTPUT_PATH = "data/geoscen/signals/macro_cb_oc_signals_v1.parquet"


def run():
    df = pd.read_parquet(INPUT_PATH).copy()

    df = df.sort_values(["bank_code", "date"]).reset_index(drop=True)

    # --- lag features ---
    df["spread_lag1"] = df.groupby("bank_code")["it_de_10y_spread"].shift(1)
    df["spread_change_lag1"] = df.groupby("bank_code")["it_de_10y_spread"].diff().shift(1)

    # --- normalize thresholds (simple but effective) ---
    tone_high = df["policy_tone"].quantile(0.7)
    unc_high = df["uncertainty"].quantile(0.7)

    # --- SIGNALS ---

    # 1. Fragmentation pressure
    df["fragmentation_pressure_flag"] = (
        (df["bank_code"] == "ECB") &
        (df["spread_change_lag1"] > 0) &
        (df["uncertainty"] > unc_high)
    ).astype(int)

    # 2. Hawkish under stress
    df["hawkish_under_stress_flag"] = (
        (df["bank_code"] == "ECB") &
        (df["policy_tone"] > tone_high) &
        (df["spread_change_lag1"] > 0)
    ).astype(int)

    # 3. Pure uncertainty regime (non-spread driven)
    df["uncertainty_without_spread_flag"] = (
        (df["uncertainty"] > unc_high) &
        (df["spread_change_lag1"] <= 0)
    ).astype(int)

    # 4. Calm regime
    df["calm_policy_flag"] = (
        (df["uncertainty"] < df["uncertainty"].quantile(0.3)) &
        (df["policy_tone"] < df["policy_tone"].quantile(0.3))
    ).astype(int)

    # 5. Divergence (ECB vs FED tone gap)
    pivot = df.pivot(index="date", columns="bank_code", values="policy_tone")

    if "ECB" in pivot.columns and "FED" in pivot.columns:
        pivot["tone_diff"] = pivot["ECB"] - pivot["FED"]
        df = df.merge(
            pivot["tone_diff"],
            on="date",
            how="left"
        )
    else:
        df["tone_diff"] = None

    df["policy_divergence_flag"] = (
        df["tone_diff"].abs() > df["tone_diff"].quantile(0.7)
    ).astype(int)

    # --- metadata ---
    df["source_layer"] = "macro_cb_oc_signals_v1"
    df["version"] = "v1"

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df.to_parquet(OUTPUT_PATH, index=False)

    print("macro_cb OC signals rows:", len(df))


if __name__ == "__main__":
    run()


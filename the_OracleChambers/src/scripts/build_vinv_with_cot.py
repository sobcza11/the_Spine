import pandas as pd

from cot_engine.utils import build_cot_monthly_features


def main():
    vinv_path_in = "data/vinv/vinv_monthly_panel_phase2.parquet"
    vinv_path_out = "data/vinv/vinv_monthly_with_cot.parquet"

    vinv = pd.read_parquet(vinv_path_in).copy()

    # Build a monthly key
    if "date" in vinv.columns:
        vinv["year_month"] = pd.to_datetime(vinv["date"]).dt.to_period("M").dt.to_timestamp()
    elif "year_month" in vinv.columns:
        vinv["year_month"] = pd.to_datetime(vinv["year_month"])
    else:
        raise ValueError("VinV panel must have 'date' or 'year_month' column for merging COT.")

    # Load COT monthly features
    cot_monthly = build_cot_monthly_features("data/cot/cot_store.parquet")

    # Merge: each equity row gets the same macro-level COT pressure for that month
    vinv_with_cot = vinv.merge(
        cot_monthly,
        on="year_month",
        how="left",
        validate="m:1",
    )

    # Save
    vinv_with_cot.to_parquet(vinv_path_out, index=False)

    print(f"[vinv_with_cot] Wrote: {vinv_path_out}")
    print(f"Shape before: {vinv.shape}, after: {vinv_with_cot.shape}")


if __name__ == "__main__":
    main()

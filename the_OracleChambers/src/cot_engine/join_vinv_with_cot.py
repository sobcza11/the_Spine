import pandas as pd

from cot_engine.utils import build_cot_monthly_features


def main() -> None:
    # Paths are relative to: the_Spine/the_OracleChambers
    # VinV panel lives in the main the_Spine data tree:
    vinv_path_in = "../data/vinv/vinv_2_0/phase2_outputs/vinv_monthly_panel_phase2.parquet"
    vinv_path_out = "../data/vinv/vinv_2_0/phase2_outputs/vinv_monthly_with_cot.parquet"

    # 1. Load VinV Phase 2 monthly panel
    vinv = pd.read_parquet(vinv_path_in).copy()

    # 2. Ensure a monthly key
    if "date" in vinv.columns:
        vinv["year_month"] = pd.to_datetime(vinv["date"]).dt.to_period("M").dt.to_timestamp()
    elif "year_month" in vinv.columns:
        vinv["year_month"] = pd.to_datetime(vinv["year_month"])
    else:
        raise ValueError("VinV panel must have 'date' or 'year_month' column.")

    # 3. Build monthly COT features from cot_store.parquet (already in data/cot)
    cot_monthly = build_cot_monthly_features("data/cot/cot_store.parquet")

    # 4. Merge: each VinV row gets the macro-level COT pressures for that month
    vinv_with_cot = vinv.merge(
        cot_monthly,
        on="year_month",
        how="left",
        validate="m:1",
    )

    # 5. Save
    vinv_with_cot.to_parquet(vinv_path_out, index=False)

    print(f"[join_vinv_with_cot] Read:  {vinv_path_in}")
    print(f"[join_vinv_with_cot] Wrote: {vinv_path_out}")
    print(f"Shape before: {vinv.shape}, after: {vinv_with_cot.shape}")


if __name__ == "__main__":
    main()


import pandas as pd

from cot_engine.utils import build_cot_monthly_features


def main():
    # 1. Load your existing macro panel
    #    🔁 Adjust this path / filename to your actual macro parquet
    macro_path_in = "data/macro/macro_signals_monthly.parquet"
    macro_path_out = "data/macro/macro_signals_with_cot.parquet"

    macro = pd.read_parquet(macro_path_in).copy()

    # Expect a date-like column; adjust name if needed
    if "date" in macro.columns:
        macro["year_month"] = pd.to_datetime(macro["date"]).dt.to_period("M").dt.to_timestamp()
    elif "year_month" in macro.columns:
        macro["year_month"] = pd.to_datetime(macro["year_month"])
    else:
        raise ValueError("Macro panel must have 'date' or 'year_month' column for merging COT.")

    # 2. Build monthly COT features
    cot_monthly = build_cot_monthly_features("data/cot/cot_store.parquet")

    # 3. Merge
    macro_with_cot = macro.merge(
        cot_monthly,
        on="year_month",
        how="left",
        validate="m:1",
    )

    # 4. Save
    macro_with_cot.to_parquet(macro_path_out, index=False)

    print(f"[macro_with_cot] Wrote: {macro_path_out}")
    print(f"Shape before: {macro.shape}, after: {macro_with_cot.shape}")


if __name__ == "__main__":
    main()

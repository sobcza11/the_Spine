import pandas as pd

input_path = r"C:\Users\Rand Sobczak Jr\_rts\3_AI\IsoVector\_py\r2_tmp\us_fx_10y_spreads.parquet"
output_path = r"C:\Users\Rand Sobczak Jr\_rts\3_AI\IsoVector\_py\r2_tmp\us_ir_diff_canonical_rebuilt.parquet"

df = pd.read_parquet(input_path).copy()

rebuilt = pd.DataFrame({
    "as_of_date": pd.to_datetime(df["as_of_date"], errors="coerce"),
    "pair": df["pair"],
    "base_ccy": df["base_ccy"],
    "quote_ccy": df["quote_ccy"],
    "base_10y_yield": pd.to_numeric(df["yld_10y_base"], errors="coerce"),
    "quote_10y_yield": pd.to_numeric(df["yld_10y_quote"], errors="coerce"),
    "diff_10y_bp": pd.to_numeric(df["yld_10y_diff"], errors="coerce") * 100.0,
    "leaf_group": "IR",
    "leaf_name": "IR_DIFF_CANONICAL",
    "source_system": df["source_system"],
    "updated_at": pd.to_datetime(df["updated_at"], errors="coerce"),
})

rebuilt = rebuilt.dropna().sort_values(["pair", "as_of_date"]).reset_index(drop=True)

rebuilt.to_parquet(output_path, index=False)

print("saved:", output_path)
print(rebuilt.tail())
print("max as_of_date:", rebuilt["as_of_date"].max())



import pandas as pd
import pathlib

# -------------------------------------
# Resolve project root (the_Spine root)
# -------------------------------------
ROOT = pathlib.Path(__file__).resolve().parents[3]
DATA_DIR = ROOT / "data" / "vinv"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# -------------------------------------
# Use returns file to get symbol/date grid
# -------------------------------------
ret_path = DATA_DIR / "vinv_returns_monthly.parquet"
if not ret_path.exists():
    raise FileNotFoundError(f"Missing returns file: {ret_path}")

df_ret = pd.read_parquet(ret_path)
print("Loaded returns:", df_ret.shape)

# Keep unique symbol/date combos
df_base = df_ret[["symbol", "date"]].drop_duplicates().copy().sort_values(["symbol", "date"])

# -------------------------------------
# Add empty fundamentals columns
# (to be filled from your VinV_1_0 / external sources)
# -------------------------------------
fund_cols = [
    "pe", "peg", "sales_multiple",
    "eps", "eps_growth_pct",
    "net_income", "sales", "sales_growth_pct",
]

for c in fund_cols:
    df_base[c] = pd.NA

outpath_parquet = DATA_DIR / "vinv_fundamentals_monthly.parquet"
outpath_csv = DATA_DIR / "vinv_fundamentals_monthly_template.csv"

df_base.to_parquet(outpath_parquet, index=False)
df_base.to_csv(outpath_csv, index=False)

print(f"\nSaved empty fundamentals parquet → {outpath_parquet}")
print(f"Saved CSV template for editing → {outpath_csv}")
print(df_base.head())

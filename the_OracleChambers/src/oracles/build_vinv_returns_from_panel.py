import pandas as pd
import pathlib

# -------------------------------------
# Resolve project root (the_Spine root)
# -------------------------------------
ROOT = pathlib.Path(__file__).resolve().parents[3]
DATA_DIR = ROOT / "data" / "vinv"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# -------------------------------------
# Load the 3-col vinv_monthly_panel.parquet
# -------------------------------------
panel_path = DATA_DIR / "vinv_monthly_panel.parquet"

if not panel_path.exists():
    raise FileNotFoundError(f"Could not find {panel_path}")

df = pd.read_parquet(panel_path)
print("Loaded 3-col panel:", df.shape)
print("Columns:", list(df.columns))

# -------------------------------------
# Rename columns to Phase 2 schema
# -------------------------------------
df_ret = df.rename(columns={
    "ticker": "symbol",
    "month": "date",
    "monthly_ret": "ret",
}).copy()

# Convert date column
df_ret["date"] = pd.to_datetime(df_ret["date"])

# Placeholder: benchmark return = 0 (you can replace later)
df_ret["bench_ret"] = 0.0

# -------------------------------------
# Save standardized returns file
# -------------------------------------
outpath = DATA_DIR / "vinv_returns_monthly.parquet"
df_ret.to_parquet(outpath, index=False)

print(f"\nSaved returns → {outpath}")
print(df_ret.head())

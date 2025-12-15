from pathlib import Path
import pandas as pd
import numpy as np

# Step 1: Build modeling view from SSOT (adds y) — time-series label
# Output: the_Spine/vinv/ssot/vinv_modeling_view_vFinal.parquet
# y = 1 if next-month ret_m > 0 else 0

ROOT = Path(__file__).resolve().parents[2]  # repo root
SSOT_PATH = ROOT / "the_Spine" / "vinv" / "ssot" / "vinv_ml_ssot_vFinal.parquet"
OUT_PATH  = ROOT / "the_Spine" / "vinv" / "ssot" / "vinv_modeling_view_vFinal.parquet"

df = pd.read_parquet(SSOT_PATH).copy()
df["date"] = pd.to_datetime(df["date"]).dt.to_period("M").dt.to_timestamp()

need = {"symbol", "date", "ret_m"}
missing = need - set(df.columns)
if missing:
    raise KeyError(f"Missing required cols in SSOT: {missing}. Found: {list(df.columns)}")

# Sort
df = df.sort_values(["symbol", "date"]).reset_index(drop=True)

# Forward return (next month) per symbol
df["ret_fwd_1m"] = df.groupby("symbol")["ret_m"].shift(-1)

# Label: next month up/down
df["y"] = (df["ret_fwd_1m"] > 0).astype(int)

# Drop rows where forward return is missing (last obs per symbol)
df = df[df["ret_fwd_1m"].notna()].copy()

# Persist
df.to_parquet(OUT_PATH, index=False)

print("Modeling view written:", OUT_PATH)
print("shape:", df.shape)
print("y distribution:", df["y"].value_counts(dropna=False).to_dict())
print("date min/max:", df["date"].min(), df["date"].max())
print("unique symbols:", df["symbol"].nunique())


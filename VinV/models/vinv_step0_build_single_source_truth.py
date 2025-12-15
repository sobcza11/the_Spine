from pathlib import Path
import pandas as pd

# Step 0: Single Source of Truth (SSOT) for VinV ML
# Output: the_Spine/vinv/ssot/vinv_ml_ssot_vFinal.parquet

ROOT = Path(__file__).resolve()
ROOT = ROOT.parents[2]

panel_path   = r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\vinv\vinv_2_0\phase2_outputs\vinv_monthly_panel_v2_with_macro.parquet"
returns_path = r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\vinv\vinv_returns_monthly.parquet"

OUT_DIR = ROOT / "the_Spine" / "vinv" / "ssot"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_PATH = OUT_DIR / "vinv_ml_ssot_vFinal.parquet"

# ---- Load
df_panel = pd.read_parquet(panel_path).copy()
df_panel["date"] = pd.to_datetime(df_panel["date"]).dt.to_period("M").dt.to_timestamp()

df_ret = pd.read_parquet(returns_path).copy()
df_ret["date"] = pd.to_datetime(df_ret["date"]).dt.to_period("M").dt.to_timestamp()

# ---- Normalize returns col to ret_m
if "ret_m" not in df_ret.columns:
    if "ret" in df_ret.columns:
        df_ret = df_ret.rename(columns={"ret": "ret_m"})
    elif "monthly_ret" in df_ret.columns:
        df_ret = df_ret.rename(columns={"monthly_ret": "ret_m"})

need_cols = {"symbol", "date", "ret_m"}
missing = need_cols - set(df_ret.columns)
if missing:
    raise KeyError(f"Returns file missing required cols: {missing}. Found: {list(df_ret.columns)}")

# ---- Join returns into panel
df = df_panel.merge(df_ret[["symbol", "date", "ret_m"]], on=["symbol", "date"], how="left")

# ---- Structural missingness flags
df["avail_ret_m"] = df["ret_m"].notna().astype(int)

# ---- Sort + persist
df = df.sort_values(["symbol", "date"]).reset_index(drop=True)
df.to_parquet(OUT_PATH, index=False)

print("SSOT written:", OUT_PATH)
print("shape:", df.shape)
print("ret_m non-null:", int(df["ret_m"].notna().sum()))
print("date min/max:", df["date"].min(), df["date"].max())
print("cols:", len(df.columns))

import pandas as pd
import pathlib

# --------------------------------------------------
# Resolve ROOT = the_Spine (one level above the_OracleChambers)
# --------------------------------------------------
ROOT = pathlib.Path(__file__).resolve().parents[3]

panel_path = ROOT / "data" / "vinv" / "vinv_2_0" / "phase2_outputs" / "vinv_monthly_panel_phase2.parquet"

print("Reading panel from:", panel_path)

df = pd.read_parquet(panel_path)

# Focus on AAPL in an early window just to confirm fundamentals + VinV
mask = (df["symbol"] == "AAPL") & (df["date"] >= "1980-12-31") & (df["date"] <= "1990-12-31")

cols = [
    "symbol", "date",
    "pe", "eps", "sales",
    "Val_z", "Qual_z", "Growth_z",
    "VinV_raw", "VinV_pct", "VinV_0_10"
]

print(df.loc[mask, cols].head(20))

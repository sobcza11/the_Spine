from pathlib import Path
import pandas as pd

# the_Spine ROOT (same trick as peek_vinv_phase2.py)
ROOT = Path(__file__).resolve().parents[3]

panel_path = ROOT / "data" / "vinv" / "vinv_2_0" / "phase2_outputs" / "vinv_monthly_panel_phase2.parquet"


df = pd.read_parquet(panel_path)

# Example: focus on Tranche 1 and a specific decade
mask = (df["tranche"] == "T1") & (df["date"].between("1980-01-01", "1990-12-31"))
df_t1 = df.loc[mask].copy()

# Simple cross-sectional VinV portfolio: equal weight each month
portfolio = (
    df_t1
    .groupby("date")
    .agg(
        VinV_0_10_mean=("VinV_0_10", "mean"),
        Val_z_mean=("Val_z", "mean"),
        Qual_z_mean=("Qual_z", "mean"),
        Growth_z_mean=("Growth_z", "mean"),
    )
    .reset_index()
)

print("=== VinV_Q2.0 Tranche 1 Summary (1980–1990) ===")
print(portfolio.head(12))  # first year
print()
print("VinV_0_10 range:", portfolio["VinV_0_10_mean"].min(), "→", portfolio["VinV_0_10_mean"].max())


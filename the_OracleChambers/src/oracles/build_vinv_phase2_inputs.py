import pandas as pd
import pathlib

# -------------------------------------
# Resolve project root (the_Spine root, not the_OracleChambers)
# -------------------------------------
# File path structure:
#   .../the_Spine/the_OracleChambers/src/oracles/build_vinv_phase2_inputs.py
#
# parents[0] = oracles
# parents[1] = src
# parents[2] = the_OracleChambers
# parents[3] = the_Spine   ← THIS is the actual project root
ROOT = pathlib.Path(__file__).resolve().parents[3]

DATA_DIR = ROOT / "data" / "vinv"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# -------------------------------------
# Locate the full VinV panel (try multiple likely paths)
# -------------------------------------
candidate_paths = [
    DATA_DIR / "vinv_monthly_panel.parquet",                           # local data
    ROOT / "vinv" / "1_0" / "monthly_panels" / "vinv_monthly_panel.parquet",  # S3 mirror
]

full_panel_path = None
for p in candidate_paths:
    if p.exists():
        full_panel_path = p
        break

if full_panel_path is None:
    raise FileNotFoundError(
        "vinv_monthly_panel.parquet not found.\n"
        "Expected one of:\n" + "\n".join(str(p) for p in candidate_paths)
    )

print(f"Using vinv_monthly_panel from: {full_panel_path}")

df = pd.read_parquet(full_panel_path)
print("Loaded full VINV panel:", df.shape)


# ============================================================
# 1. BUILD vinv_fundamentals_monthly.parquet
# ============================================================

fund_cols = [
    "symbol", "date",
    "pe", "peg", "sales_multiple",
    "eps", "eps_growth_pct",
    "net_income", "sales", "sales_growth_pct"
]

missing = [c for c in fund_cols if c not in df.columns]

if missing:
    print("\n[build_vinv_phase2_inputs] The full panel does NOT contain the expected fundamentals.")
    print("Available columns:", list(df.columns))
    print("Missing expected columns:", missing)
    raise SystemExit(
        "\nPhase 2 needs a richer VinV panel (with fundamentals like pe, eps, sales, etc.).\n"
        "Right now data/vinv/vinv_monthly_panel.parquet only has these columns.\n"
        "Once you point me to the file that *does* contain fundamentals, we’ll wire that in."
    )

df_fund = df[fund_cols].dropna(subset=["symbol", "date"]).copy()

fund_path = DATA_DIR / "vinv_fundamentals_monthly.parquet"
df_fund.to_parquet(fund_path, index=False)
print(f"Saved fundamentals → {fund_path}")


# ============================================================
# 2. BUILD vinv_cot_weekly.parquet
#    (If COT already monthly inside your panel, we extract it.
#     If not, we build a placeholder for now.)
# ============================================================

cot_cols = [
    "symbol", "date",
    "non_com_long", "non_com_short", "open_interest"
]

existing_cot_cols = [c for c in cot_cols if c in df.columns]

if set(cot_cols).issubset(df.columns):
    # extract from master panel
    df_cot_weekly = df[cot_cols].dropna(subset=["non_com_long"]).copy()
else:
    # provide a placeholder structure (required for Phase 2)
    df_cot_weekly = pd.DataFrame(columns=cot_cols)
    print("\nWARNING: No COT data found in monthly panel!")
    print("A placeholder file has been created. Replace with real COT data.\n")

cot_path = DATA_DIR / "vinv_cot_weekly.parquet"
df_cot_weekly.to_parquet(cot_path, index=False)
print(f"Saved COT weekly → {cot_path}")


# ============================================================
# 3. BUILD vinv_returns_monthly.parquet
# ============================================================

ret_cols = [
    "symbol", "date",
    "ret",          # stock return
    "bench_ret"     # benchmark return (e.g., IVV)
]

df_ret = df[ret_cols].dropna(subset=["ret", "bench_ret"]).copy()

ret_path = DATA_DIR / "vinv_returns_monthly.parquet"
df_ret.to_parquet(ret_path, index=False)
print(f"Saved returns → {ret_path}")


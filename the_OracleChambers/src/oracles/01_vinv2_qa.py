from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

"""
01_vinv2_qa.py

Lightweight QA for VinV_2.0 (less COT):
- Checks basic health of the VinV Phase 2 panel
- Writes tranche-by-year coverage
- Writes VinV_0_10 by tranche & decade
- Optionally inspects VinV+Macro joined panel
"""

# ------------------------------------------------------------------------------------
# Paths
# ------------------------------------------------------------------------------------
THIS_FILE = Path(__file__).resolve()
ORACLECHAMBERS_ROOT = THIS_FILE.parents[2]     # ...\the_Spine\the_OracleChambers
SPINE_ROOT = ORACLECHAMBERS_ROOT.parent        # ...\the_Spine

print(f"[QA] Detected SPINE_ROOT:          {SPINE_ROOT}")
print(f"[QA] Detected ORACLECHAMBERS_ROOT: {ORACLECHAMBERS_ROOT}")

# VinV lives under the_Spine/data/...
VINV_PHASE2_PANEL = (
    SPINE_ROOT
    / "data"
    / "vinv"
    / "vinv_2_0"
    / "phase2_outputs"
    / "vinv_monthly_panel_phase2.parquet"
)

VINV_WITH_MACRO_PANEL = (
    SPINE_ROOT
    / "data"
    / "vinv"
    / "vinv_2_0"
    / "phase2_outputs"
    / "vinv_monthly_panel_v2_with_macro.parquet"
)

# QA outputs live under the_OracleChambers/data/...
QA_DIR = (
    ORACLECHAMBERS_ROOT
    / "data"
    / "vinv"
    / "vinv_2_0"
    / "qa"
)
QA_DIR.mkdir(parents=True, exist_ok=True)

print(f"[QA] VinV Phase2 panel path:       {VINV_PHASE2_PANEL}")
print(f"[QA] VinV+Macro panel path:        {VINV_WITH_MACRO_PANEL}")
print(f"[QA] QA output directory:          {QA_DIR}")



# ------------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------------
def safe_read_parquet(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"[QA] {label} not found at {path}")
    print(f"[QA] Loading {label} from {path}")
    return pd.read_parquet(path)


def plot_sample_timeseries(df: pd.DataFrame, symbol: str = "AAPL") -> None:
    """Simple VinV_0_10 timeseries plot for a given symbol."""
    subset = df[df["symbol"] == symbol].copy()
    if subset.empty:
        print(f"[QA] No rows for symbol={symbol}, skipping plot")
        return

    subset = subset.sort_values("date")
    plt.figure(figsize=(10, 4))
    plt.plot(subset["date"], subset["VinV_0_10"])
    plt.title(f"VinV_0_10 over time — {symbol}")
    plt.xlabel("Date")
    plt.ylabel("VinV_0_10 (0–10)")
    plt.tight_layout()

    out_path = QA_DIR / f"vinv_timeseries_{symbol}.png"
    plt.savefig(out_path)
    plt.close()
    print(f"[QA] Saved sample timeseries plot → {out_path}")


# ------------------------------------------------------------------------------------
# Main QA runner
# ------------------------------------------------------------------------------------
def run_vinv_qa() -> None:
    print(f"[QA] Detected SPINE_ROOT:          {SPINE_ROOT}")
    print(f"[QA] VinV Phase2 panel path:       {VINV_PHASE2_PANEL}")
    print(f"[QA] VinV+Macro panel path:        {VINV_WITH_MACRO_PANEL}")
    print(f"[QA] QA output directory:          {QA_DIR}")

    # 1) Load VinV Phase2 panel
    df_vinv = safe_read_parquet(VINV_PHASE2_PANEL, "VinV Phase2 panel")
    print(f"[QA] Loaded VinV Phase2 panel: {df_vinv.shape}\n")

    # Basic health checks
    print("[QA] === Basic health checks: VinV Phase2 panel ===")
    print(f"  • Symbols: {df_vinv['symbol'].nunique()}")
    print(f"  • Date range: {df_vinv['date'].min()} → {df_vinv['date'].max()}")

    for col in ["VinV_raw", "VinV_pct", "VinV_0_10", "Val_z", "Qual_z", "Growth_z", "pe", "eps", "sales"]:
        if col in df_vinv.columns:
            print(f"  • Nulls in {col}: {df_vinv[col].isna().sum()}")

    print("\n[QA] Head:")
    print(df_vinv.head())
    print("\n[QA] Tail:")
    print(df_vinv.tail())

    # 2) Coverage by tranche + year
    df_cov = (
        df_vinv.assign(year=df_vinv["date"].dt.year)
        .groupby(["tranche", "year"])
        .size()
        .reset_index(name="n_obs")
    )

    print("\n[QA] Tranche-by-year coverage (first 20 rows):")
    print(df_cov.head(20))

    cov_out = QA_DIR / "vinv_tranche_coverage"
    df_cov.to_csv(str(cov_out) + ".csv", index=False)
    df_cov.to_parquet(str(cov_out) + ".parquet", index=False)
    print(f"[QA] Saved tranche coverage → {cov_out}.*")

    # 3) VinV_0_10 by tranche & decade
    df_vinv["decade"] = (df_vinv["date"].dt.year // 10) * 10
    df_decade = (
        df_vinv.groupby(["tranche", "decade"])["VinV_0_10"]
        .agg(["count", "mean", "min", "max"])
        .reset_index()
    )

    print("\n[QA] VinV_0_10 by tranche & decade:")
    print(df_decade)

    dec_out = QA_DIR / "vinv_vinv0_10_decade_summary"
    df_decade.to_csv(str(dec_out) + ".csv", index=False)
    df_decade.to_parquet(str(dec_out) + ".parquet", index=False)
    print(f"[QA] Saved decade summary → {dec_out}.*")

    # 4) Sample timeseries plot (best effort)
    try:
        plot_sample_timeseries(df_vinv, symbol="AAPL")
    except Exception as e:
        print(f"[QA] Skipping sample plot due to error: {e}")

    # 5) Optional: inspect VinV+Macro panel if present
    if VINV_WITH_MACRO_PANEL.exists():
        df_macro = safe_read_parquet(VINV_WITH_MACRO_PANEL, "VinV+Macro panel")
        print(f"\n[QA] Loaded VinV+Macro panel: {df_macro.shape}")
        print(df_macro.head())
    else:
        print(f"\n[QA] VinV+Macro panel NOT found at {VINV_WITH_MACRO_PANEL}")


# ------------------------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------------------------
if __name__ == "__main__":
    run_vinv_qa()

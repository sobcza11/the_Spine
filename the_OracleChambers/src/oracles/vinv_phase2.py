"""
VinV_2.0 – Phase 2: Factor Construction & Scoring (Tranche-Aware)

Inputs (minimum):
    df_fundamentals: monthly panel with columns
        - symbol
        - date (pd.Timestamp, month-end)
        - pe, peg, sales_multiple
        - eps, eps_growth_pct
        - net_income, sales, sales_growth_pct
        - (optionally: equity, revenue if you want ROE / margin)

    df_cot: weekly COT data with columns
        - symbol
        - date (pd.Timestamp, weekly)
        - non_com_long
        - non_com_short
        - open_interest

    df_returns: monthly returns panel with columns
        - symbol
        - date
        - ret
        - bench_ret  (benchmark return for excess calc)

Outputs:
    df_vinv_panel: monthly panel with
        - symbol, date, tranche
        - Val_z, Qual_z, Growth_z, COT_z
        - VinV_raw, VinV_pct, VinV_0_10
        - plus any original fundamentals you want to carry through
"""

import numpy as np
import pandas as pd
from dataclasses import dataclass
from typing import Dict, List


# -----------------------------
# Config
# -----------------------------

@dataclass
class Tranche:
    name: str
    start_year: int   # inclusive
    end_year: int     # exclusive


TRANCHES: List[Tranche] = [
    Tranche("T1", 1970, 1990),
    Tranche("T2", 1990, 2010),
    Tranche("T3", 2010, 2025),
    Tranche("T4", 2025, 2031),  # 2030 inclusive
]

FACTOR_COLUMNS = ["Val_z", "Qual_z", "Growth_z", "COT_z"]
RIDGE_L2 = 1e-4  # small ridge penalty for stability


# -----------------------------
# Utility Functions
# -----------------------------

def assign_tranche(year: int) -> str:
    """
    Map calendar year → tranche name.
    """
    for tr in TRANCHES:
        if tr.start_year <= year < tr.end_year:
            return tr.name
    return "OUT_OF_SAMPLE"


def zscore_cross_sectional(df: pd.DataFrame, col: str, group_col: str = "date") -> pd.Series:
    """
    Cross-sectional z-score by group_col (usually 'date').
    """
    def _z(g):
        x = g[col]
        mu = x.mean()
        sd = x.std(ddof=0)
        if sd == 0 or np.isnan(sd):
            return pd.Series(0.0, index=g.index)
        return (x - mu) / sd

    return df.groupby(group_col, group_keys=False).apply(_z)


def rolling_zscore(series: pd.Series, window: int) -> pd.Series:
    """
    Time-series rolling z-score.
    """
    roll_mean = series.rolling(window).mean()
    roll_std = series.rolling(window).std(ddof=0)
    z = (series - roll_mean) / roll_std
    return z


# -----------------------------
# Step 1 – COT Sentiment
# -----------------------------

def build_cot_sentiment(df_cot: pd.DataFrame, window_weeks: int = 52) -> pd.DataFrame:
    """
    Build monthly COT sentiment factor:
        - net non-commercial % of OI
        - rolling 52w z-score
        - contrarian sign
        - monthly last-observation alignment

    If df_cot is empty or missing required columns, returns an empty
    [symbol, date, COT_z] frame so the rest of Phase 2 can run without COT.
    """
    # --- Handle empty / placeholder COT cleanly ---
    required_cols = {"symbol", "date", "non_com_long", "non_com_short", "open_interest"}
    if df_cot is None or df_cot.empty or not required_cols.issubset(df_cot.columns):
        return pd.DataFrame(columns=["symbol", "date", "COT_z"])

    df = df_cot.copy()
    # Ensure datetime
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])

    if df.empty:
        return pd.DataFrame(columns=["symbol", "date", "COT_z"])

    df = df.sort_values(["symbol", "date"])

    # Net non-commercial as % of open interest
    df["net_nc"] = df["non_com_long"] - df["non_com_short"]
    df["net_nc_pct_oi"] = df["net_nc"] / df["open_interest"].replace(0, np.nan)

    # Rolling z per symbol
    df["cot_z_52w"] = (
        df.groupby("symbol", group_keys=False)["net_nc_pct_oi"]
          .apply(lambda s: rolling_zscore(s, window_weeks))
    )

    # Contrarian sentiment
    df["COT_z"] = -1.0 * df["cot_z_52w"]

    # Take last COT obs of each month per symbol
    df["year_month"] = df["date"].dt.to_period("M")
    df_monthly = (
        df.dropna(subset=["COT_z"])
          .sort_values("date")
          .groupby(["symbol", "year_month"])
          .tail(1)
    )
    df_monthly["date"] = df_monthly["year_month"].dt.to_timestamp("M")
    return df_monthly[["symbol", "date", "COT_z"]]


# -----------------------------
# Step 2 – Fundamentals → Val/Qual/Growth Factors
# -----------------------------

def build_factor_blocks(df_fund: pd.DataFrame) -> pd.DataFrame:
    """
    Take raw fundamentals and construct standardized factor blocks:
        Val_z, Qual_z, Growth_z

    Expects df_fund to already be at monthly frequency with:
        pe, peg, sales_multiple,
        eps, eps_growth_pct,
        net_income, sales, sales_growth_pct

    You can augment with ROE or margins if underlying data exists.
    """

    df = df_fund.copy()

    # Defensive: ensure numeric types
    numeric_cols = [
        "pe", "peg", "sales_multiple",
        "eps", "eps_growth_pct",
        "net_income", "sales", "sales_growth_pct"
    ]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Example derived quality metric: profit margin proxy
    # (If revenue is not available, skip this or replace with your own)
    if "sales" in df.columns and "net_income" in df.columns:
        df["profit_margin"] = df["net_income"] / df["sales"].replace(0, np.nan)
    else:
        df["profit_margin"] = np.nan

    # Value submetrics (higher = cheaper after sign flip)
    df["val_pe"] = -1.0 * df["pe"]              # high P/E → low value
    df["val_peg"] = -1.0 * df["peg"]
    df["val_sales_mult"] = -1.0 * df["sales_multiple"]

    # Quality submetrics
    df["qual_eps"] = df["eps"]
    df["qual_eps_growth"] = df["eps_growth_pct"]
    df["qual_margin"] = df["profit_margin"]

    # Growth submetrics
    df["growth_sales_growth"] = df["sales_growth_pct"]
    df["growth_peg"] = -1.0 * df["peg"]  # PEG low = "cheap growth"; optional dual use

    # Cross-sectional z-scores for each submetric
    val_sub = ["val_pe", "val_peg", "val_sales_mult"]
    qual_sub = ["qual_eps", "qual_eps_growth", "qual_margin"]
    growth_sub = ["growth_sales_growth", "growth_peg"]

    for col in val_sub + qual_sub + growth_sub:
        df[f"{col}_z"] = zscore_cross_sectional(df, col, group_col="date")

    # Aggregate into blocks: simple average of sub-z’s (can be weighted later if desired)
    df["Val_z"] = df[[f"{c}_z" for c in val_sub]].mean(axis=1, skipna=True)
    df["Qual_z"] = df[[f"{c}_z" for c in qual_sub]].mean(axis=1, skipna=True)
    df["Growth_z"] = df[[f"{c}_z" for c in growth_sub]].mean(axis=1, skipna=True)

    return df


# -----------------------------
# Step 3 – Merge Returns & Build Forward Excess
# -----------------------------

def attach_forward_excess_returns(df: pd.DataFrame, df_ret: pd.DataFrame) -> pd.DataFrame:
    """
    Attach 1-month-ahead excess return rx_fwd_1m for cross-sectional regressions.
    df_ret should have columns: symbol, date, ret, bench_ret
    """
    df_r = df_ret.copy()
    df_r = df_r.sort_values(["symbol", "date"])
    df_r["excess_ret"] = df_r["ret"] - df_r["bench_ret"]

    # Shift by 1 month forward per symbol
    df_r["rx_fwd_1m"] = df_r.groupby("symbol")["excess_ret"].shift(-1)

    # Merge back into feature set
    df_merged = df.merge(
        df_r[["symbol", "date", "rx_fwd_1m"]],
        on=["symbol", "date"],
        how="left"
    )
    return df_merged


# -----------------------------
# Step 4 – Estimate Tranche Weights
# -----------------------------

def estimate_tranche_weights(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each tranche, run monthly cross-sectional ridge regressions:
        rx_fwd_1m = beta_val * Val_z + beta_qual * Qual_z + beta_growth * Growth_z + beta_cot * COT_z

    Returns:
        DataFrame with index=tranche, columns=FACTOR_COLUMNS
    """
    weights = []

    for tr in TRANCHES:
        df_tr = df[df["tranche"] == tr.name].copy()
        betas_list = []

        for dt, g in df_tr.groupby("date"):
            g = g.dropna(subset=["rx_fwd_1m"] + FACTOR_COLUMNS)
            if len(g) < len(FACTOR_COLUMNS) + 5:
                continue

            X = g[FACTOR_COLUMNS].values
            y = g["rx_fwd_1m"].values

            # Ridge solution
            XtX = X.T @ X + RIDGE_L2 * np.eye(X.shape[1])
            Xty = X.T @ y
            beta = np.linalg.solve(XtX, Xty)
            betas_list.append(beta)

        if not betas_list:
            # No data; default to equal weights
            w = np.ones(len(FACTOR_COLUMNS)) / len(FACTOR_COLUMNS)
        else:
            betas = np.vstack(betas_list)
            w = betas.mean(axis=0)
            # Normalize by L1 norm for interpretability
            norm = np.sum(np.abs(w))
            if norm > 0:
                w = w / norm

        weights.append(pd.Series(w, index=FACTOR_COLUMNS, name=tr.name))

    df_w = pd.DataFrame(weights)
    return df_w


# -----------------------------
# Step 5 – Apply Tranche Weights → VinV_raw, VinV_pct, VinV_0_10
# -----------------------------

def apply_tranche_weights(df, df_tranche_weights):
    df = df.copy()

    # Merge weights onto the panel
    df = df.merge(
        df_tranche_weights.reset_index().rename(columns={"index": "tranche"}),
        on="tranche",
        how="left",
        suffixes=("", "_w"),
    )

    # For each row, build a weighted VinV, treating missing factors as 0 (neutral)
    def _row_score(row):
        factors = []
        weights = []

        # Value
        if pd.notna(row["Val_z"]):
            factors.append(row["Val_z"])
            weights.append(row["Val_z_w"])

        # Quality
        if pd.notna(row["Qual_z"]):
            factors.append(row["Qual_z"])
            weights.append(row["Qual_z_w"])

        # Growth
        if pd.notna(row["Growth_z"]):
            factors.append(row["Growth_z"])
            weights.append(row["Growth_z_w"])

        # COT (optional)
        if pd.notna(row.get("COT_z", float("nan"))):
            factors.append(row["COT_z"])
            weights.append(row["COT_z_w"])

        if not factors:  # nothing available
            return float("nan")

        w_sum = sum(weights) if sum(weights) != 0 else 1.0
        return sum(f * w for f, w in zip(factors, weights)) / w_sum

    df["VinV_raw"] = df.apply(_row_score, axis=1)

    # rank → percentile → 0–10
    df["VinV_pct"] = df.groupby("date")["VinV_raw"].rank(pct=True)
    df["VinV_0_10"] = df["VinV_pct"] * 10.0

    return df


# -----------------------------
# Phase 2 Orchestrator
# -----------------------------

def run_vinv_phase2(
    df_fundamentals: pd.DataFrame,
    df_cot: pd.DataFrame,
    df_returns: pd.DataFrame
) -> (pd.DataFrame, pd.DataFrame):
    """
    Run full Phase 2 pipeline:
        1. Build COT sentiment (monthly)
        2. Build Val/Qual/Growth factors from fundamentals
        3. Merge COT & factors
        4. Attach forward excess returns
        5. Assign tranches
        6. Estimate tranche weights
        7. Apply weights to get VinV_raw / VinV_pct / VinV_0_10

    Returns:
        df_vinv_panel, df_tranche_weights
    """

    # 1) COT sentiment
    df_cot_monthly = build_cot_sentiment(df_cot)

    # 2) Factor blocks
    df_factors = build_factor_blocks(df_fundamentals)

    # 3) Merge COT into factor panel
    df = df_factors.merge(
        df_cot_monthly,
        on=["symbol", "date"],
        how="left"
    )

    # 4) Attach forward excess returns
    df = attach_forward_excess_returns(df, df_returns)

    # 5) Assign tranche by year
    df["year"] = df["date"].dt.year
    df["tranche"] = df["year"].apply(assign_tranche)

    # 6) Estimate tranche weights
    df_weights = estimate_tranche_weights(df)

    # 7) Apply weights → VinV scores
    df_vinv = apply_tranche_weights(df, df_weights)

    # Final tidy columns – keep what you need
    keep_cols = [
        "symbol", "date", "tranche",
        "Val_z", "Qual_z", "Growth_z", "COT_z",
        "VinV_raw", "VinV_pct", "VinV_0_10",
        # optionally carry original fundamentals:
        "pe", "peg", "sales_multiple",
        "eps", "eps_growth_pct", "sales_growth_pct",
        "net_income", "sales",
    ]
    keep_cols = [c for c in keep_cols if c in df_vinv.columns]
    df_vinv = df_vinv[keep_cols].sort_values(["symbol", "date"])

    return df_vinv, df_weights


# -----------------------------
# Example usage stub (remove or adapt in your repo)
# -----------------------------

if __name__ == "__main__":
    import pathlib
    import pandas as pd

    # Resolve the_Spine root
    ROOT = pathlib.Path(__file__).resolve().parents[3]
    DATA_DIR = ROOT / "data" / "vinv"

    # ---- 1. Load fundamentals & returns ----
    fund_path = DATA_DIR / "vinv_fundamentals_monthly.parquet"
    ret_path = DATA_DIR / "vinv_returns_monthly.parquet"
    cot_path = DATA_DIR / "vinv_cot_weekly.parquet"

    print(f"Loading fundamentals from: {fund_path}")
    print(f"Loading returns from:      {ret_path}")

    df_fundamentals = pd.read_parquet(fund_path)
    df_returns = pd.read_parquet(ret_path)

    # ---- 2. Initialize COT as empty, then try to load if present ----
    df_cot = pd.DataFrame(
        columns=["symbol", "date", "non_com_long", "non_com_short", "open_interest"]
    )

    if cot_path.exists():
        try:
            print(f"Loading COT from:          {cot_path}")
            df_cot = pd.read_parquet(cot_path)
        except Exception as e:
            print(f"Failed to read COT parquet ({e}). Using empty placeholder instead.")
    else:
        print("No COT file found – using empty placeholder (COT_z will be NaN).")

    # ---- 3. Run Phase 2 pipeline ----
    df_vinv_panel, df_tranche_weights = run_vinv_phase2(
        df_fundamentals=df_fundamentals,
        df_cot=df_cot,
        df_returns=df_returns,
    )

    # ---- 4. Save outputs ----
    out_dir = DATA_DIR / "vinv_2_0" / "phase2_outputs"
    out_dir.mkdir(parents=True, exist_ok=True)

    panel_out = out_dir / "vinv_monthly_panel_phase2.parquet"
    weights_out = out_dir / "vinv_tranche_weights.csv"

    df_vinv_panel.to_parquet(panel_out, index=False)
    df_tranche_weights.to_csv(weights_out)

    print("\n[VinV Phase 2] Completed.")
    print(f"  → Panel saved to:   {panel_out}")
    print(f"  → Weights saved to: {weights_out}\n")

    print("[VinV Phase 2] Tranche weights:")
    print(df_tranche_weights)

    print("\n[VinV Phase 2] Sample of final panel:")
    print(df_vinv_panel.tail(10))

    pass

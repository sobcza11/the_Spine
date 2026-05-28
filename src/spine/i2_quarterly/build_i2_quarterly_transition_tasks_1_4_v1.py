from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd
import numpy as np


def find_col(df, candidates):
    cols = {c.lower(): c for c in df.columns}
    for name in candidates:
        if name.lower() in cols:
            return cols[name.lower()]
    for c in df.columns:
        lc = c.lower()
        if any(x.lower() in lc for x in candidates):
            return c
    return None


def classify_pressure(x):
    if x >= 0.70: return "systemic_quarterly_pressure"
    if x >= 0.55: return "fragile_quarterly_pressure"
    if x >= 0.40: return "elevated_quarterly_pressure"
    if x >= 0.25: return "watch_quarterly_pressure"
    return "stable_quarterly_pressure"


def build_i2_quarterly_transition_tasks_1_4_v1():
    root = Path.cwd()
    src = root / "data" / "i2_quarterly" / "i2_quarterly_statement_panel_v1.parquet"
    out = root / "data" / "i2_quarterly"
    out.mkdir(parents=True, exist_ok=True)

    if not src.exists():
        raise FileNotFoundError(f"Missing quarterly panel: {src}")

    df = pd.read_parquet(src).copy()
    df["statement_date"] = pd.to_datetime(df["statement_date"], errors="coerce")
    df = df.dropna(subset=["symbol", "statement_date"])
    df = df.sort_values(["symbol", "statement_date"]).reset_index(drop=True)

    debt_col = find_col(df, ["Total Debt", "Long Term Debt", "Short Term Debt"])
    equity_col = find_col(df, ["Total Equity", "Shareholders Equity"])
    revenue_col = find_col(df, ["Revenue"])
    gross_profit_col = find_col(df, ["Gross Profit"])
    current_assets_col = find_col(df, ["Total Current Assets", "Current Assets"])
    current_liabilities_col = find_col(df, ["Total Current Liabilities", "Current Liabilities"])
    ebit_col = find_col(df, ["Operating Income", "Operating Income (Loss)", "EBIT"])
    interest_col = find_col(df, ["Interest Expense", "Interest Expense, Net"])

    if debt_col:
        df["quarterly_debt"] = pd.to_numeric(df[debt_col], errors="coerce")
    else:
        df["quarterly_debt"] = np.nan

    if equity_col:
        df["quarterly_equity"] = pd.to_numeric(df[equity_col], errors="coerce")
    else:
        df["quarterly_equity"] = np.nan

    df["q_debt_to_equity"] = df["quarterly_debt"] / df["quarterly_equity"].replace(0, np.nan)
    df["q_debt_acceleration"] = df.groupby("symbol")["q_debt_to_equity"].diff()
    df["q_debt_acceleration_pressure"] = df["q_debt_acceleration"].clip(lower=0).fillna(0)

    if revenue_col and gross_profit_col:
        df["q_gross_margin"] = (
            pd.to_numeric(df[gross_profit_col], errors="coerce") /
            pd.to_numeric(df[revenue_col], errors="coerce").replace(0, np.nan)
        )
    else:
        df["q_gross_margin"] = np.nan

    df["q_margin_change"] = df.groupby("symbol")["q_gross_margin"].diff()
    df["q_margin_compression"] = (-df["q_margin_change"]).clip(lower=0).fillna(0)

    if current_assets_col and current_liabilities_col:
        df["q_current_ratio"] = (
            pd.to_numeric(df[current_assets_col], errors="coerce") /
            pd.to_numeric(df[current_liabilities_col], errors="coerce").replace(0, np.nan)
        )
    else:
        df["q_current_ratio"] = np.nan

    df["q_liquidity_change"] = df.groupby("symbol")["q_current_ratio"].diff()
    df["q_liquidity_deterioration"] = (-df["q_liquidity_change"]).clip(lower=0).fillna(0)

    if ebit_col and interest_col:
        interest = pd.to_numeric(df[interest_col], errors="coerce").abs().replace(0, np.nan)
        df["q_interest_coverage"] = pd.to_numeric(df[ebit_col], errors="coerce") / interest
    else:
        df["q_interest_coverage"] = np.nan

    df["q_interest_coverage_change"] = df.groupby("symbol")["q_interest_coverage"].diff()
    df["q_interest_coverage_drift"] = (-df["q_interest_coverage_change"]).clip(lower=0).fillna(0)

    pressure_cols = [
        "q_debt_acceleration_pressure",
        "q_margin_compression",
        "q_liquidity_deterioration",
        "q_interest_coverage_drift",
    ]

    for col in pressure_cols:
        s = pd.to_numeric(df[col], errors="coerce")
        cap = s.quantile(0.95)
        if pd.notna(cap) and cap > 0:
            df[col + "_norm"] = (s.clip(upper=cap) / cap).fillna(0).round(4)
        else:
            df[col + "_norm"] = 0.0

    df["q_transition_pressure_1_4"] = df[[c + "_norm" for c in pressure_cols]].mean(axis=1).round(4)
    df["q_transition_state_1_4"] = df["q_transition_pressure_1_4"].apply(classify_pressure)

    summary = {
        "component": "i2_quarterly_transition_tasks_1_4_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "rows": int(len(df)),
        "symbols": int(df["symbol"].nunique()),
        "average_transition_pressure": round(float(df["q_transition_pressure_1_4"].mean()), 4),
        "debt_col": debt_col,
        "equity_col": equity_col,
        "revenue_col": revenue_col,
        "gross_profit_col": gross_profit_col,
        "current_assets_col": current_assets_col,
        "current_liabilities_col": current_liabilities_col,
        "ebit_col": ebit_col,
        "interest_col": interest_col,
        "status": "i2_quarterly_transition_tasks_1_4_complete",
    }

    df.to_parquet(out / "i2_quarterly_transition_tasks_1_4_v1.parquet", index=False)
    df.to_json(out / "i2_quarterly_transition_tasks_1_4_sample_v1.json", orient="records", indent=2, date_format="iso")

    with open(out / "i2_quarterly_transition_tasks_1_4_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("I2 Quarterly Transition Tasks 1-4 complete")
    print("Rows:", summary["rows"])
    print("Symbols:", summary["symbols"])
    print("Average pressure:", summary["average_transition_pressure"])


if __name__ == "__main__":
    build_i2_quarterly_transition_tasks_1_4_v1()

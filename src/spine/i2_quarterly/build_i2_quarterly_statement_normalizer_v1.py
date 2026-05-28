from pathlib import Path
from datetime import datetime, UTC
import pandas as pd
import numpy as np
import json


def build_i2_quarterly_statement_normalizer_v1():

    root = Path.cwd()

    out = root / "data" / "i2_quarterly"
    out.mkdir(parents=True, exist_ok=True)

    # =====================================================
    # SOURCE PATHS
    # =====================================================

    income_path = root / "data" / "fundamentals" / "simfin" / "income_quarterly.parquet"
    balance_path = root / "data" / "fundamentals" / "simfin" / "balance_quarterly.parquet"
    cashflow_path = root / "data" / "fundamentals" / "simfin" / "cashflow_quarterly.parquet"

    # =====================================================
    # LOAD
    # =====================================================

    income = pd.read_parquet(income_path)
    balance = pd.read_parquet(balance_path)
    cashflow = pd.read_parquet(cashflow_path)

    # =====================================================
    # NORMALIZE DATES
    # =====================================================

    for df in [income, balance, cashflow]:

        if "date" in df.columns:
            df["statement_date"] = pd.to_datetime(df["date"])

        elif "report_date" in df.columns:
            df["statement_date"] = pd.to_datetime(df["report_date"])

    # =====================================================
    # STANDARDIZE
    # =====================================================

    keep_cols = [
        "symbol",
        "statement_date"
    ]

    income_cols = [
        c for c in income.columns
        if c not in keep_cols
    ]

    balance_cols = [
        c for c in balance.columns
        if c not in keep_cols
    ]

    cashflow_cols = [
        c for c in cashflow.columns
        if c not in keep_cols
    ]

    panel = (
        income
        .merge(
            balance,
            on=["symbol", "statement_date"],
            how="outer",
            suffixes=("", "_balance")
        )
        .merge(
            cashflow,
            on=["symbol", "statement_date"],
            how="outer",
            suffixes=("", "_cashflow")
        )
    )

    # =====================================================
    # SORT
    # =====================================================

    panel = (
        panel
        .sort_values(["symbol", "statement_date"])
        .reset_index(drop=True)
    )

    # =====================================================
    # REPORTING METADATA
    # =====================================================

    panel["reporting_frequency"] = "quarterly"

    counts = (
        panel
        .groupby("symbol")
        .size()
        .rename("quarter_count")
        .reset_index()
    )

    panel = panel.merge(
        counts,
        on="symbol",
        how="left"
    )

    # =====================================================
    # FORWARD FILL
    # =====================================================

    panel = panel.sort_values(["symbol", "statement_date"])

    numeric_cols = panel.select_dtypes(include=np.number).columns.tolist()

    numeric_cols = [
        c for c in numeric_cols
        if c not in ["quarter_count"]
    ]

    panel[numeric_cols] = (
        panel
        .groupby("symbol")[numeric_cols]
        .ffill(limit=3)
    )

    # =====================================================
    # REPORTING CONFIDENCE
    # =====================================================

    panel["reporting_confidence"] = np.where(
        panel["quarter_count"] >= 12,
        1.0,
        np.where(
            panel["quarter_count"] >= 8,
            0.75,
            0.5
        )
    )

    # =====================================================
    # SAVE
    # =====================================================

    parquet_path = out / "i2_quarterly_statement_panel_v1.parquet"

    panel.to_parquet(parquet_path, index=False)

    panel.head(500).to_json(
        out / "i2_quarterly_statement_sample_v1.json",
        orient="records",
        indent=2,
        date_format="iso"
    )

    summary = {
        "component": "i2_quarterly_statement_normalizer_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "rows": int(len(panel)),
        "symbols": int(panel["symbol"].nunique()),
        "min_date": str(panel["statement_date"].min()),
        "max_date": str(panel["statement_date"].max()),
        "avg_reporting_confidence": round(
            float(panel["reporting_confidence"].mean()),
            4
        ),
        "status": "quarterly_foundation_complete"
    }

    with open(
        out / "i2_quarterly_statement_summary_v1.json",
        "w",
        encoding="utf-8"
    ) as f:
        json.dump(summary, f, indent=2)

    print("I2 Quarterly Statement Foundation complete")
    print("Rows:", summary["rows"])
    print("Symbols:", summary["symbols"])
    print("Confidence:", summary["avg_reporting_confidence"])
    print("OUTPUT:", parquet_path)


if __name__ == "__main__":
    build_i2_quarterly_statement_normalizer_v1()

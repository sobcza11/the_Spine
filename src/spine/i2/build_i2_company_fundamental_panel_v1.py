from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


METRIC_HINTS = {
    "revenue": ["revenue", "sales"],
    "gross_profit": ["gross profit"],
    "operating_income": ["operating income", "ebit"],
    "net_income": ["net income"],
    "assets": ["total assets"],
    "liabilities": ["total liabilities"],
    "equity": ["total equity", "shareholders equity"],
    "cash": ["cash"],
    "debt": ["debt"],
    "current_assets": ["current assets"],
    "current_liabilities": ["current liabilities"],
    "operating_cash_flow": ["operating cash flow", "net cash from operating"],
    "capex": ["capital expenditure", "capex"],
}


def find_col(df, hints):
    cols = list(df.columns)
    low = {c.lower(): c for c in cols}
    for hint in hints:
        for lc, c in low.items():
            if hint in lc:
                return c
    return None


def build_i2_company_fundamental_panel_v1():
    root = Path.cwd()
    src = root / "data" / "i2" / "i2_statement_normalized_v1.parquet"
    out = root / "data" / "i2"
    out.mkdir(parents=True, exist_ok=True)

    if not src.exists():
        raise FileNotFoundError(f"Missing normalized statements: {src}")

    df = pd.read_parquet(src).copy()

    if "statement_date" not in df.columns:
        raise KeyError("statement_date missing from normalized statements")

    df["statement_date"] = pd.to_datetime(df["statement_date"], errors="coerce")
    df["year"] = df["statement_date"].dt.year

    cols = {}
    for metric, hints in METRIC_HINTS.items():
        col = find_col(df, hints)
        if col:
            cols[metric] = col

    keep = ["symbol", "year"]
    agg = {}

    for metric, col in cols.items():
        df[metric] = pd.to_numeric(df[col], errors="coerce")
        keep.append(metric)
        agg[metric] = "last"

    panel = (
        df[keep]
        .dropna(subset=["symbol", "year"])
        .sort_values(["symbol", "year"])
        .groupby(["symbol", "year"], as_index=False)
        .agg(agg)
    )

    if "revenue" in panel.columns and "gross_profit" in panel.columns:
        panel["gross_margin"] = panel["gross_profit"] / panel["revenue"]

    if "operating_income" in panel.columns and "revenue" in panel.columns:
        panel["operating_margin"] = panel["operating_income"] / panel["revenue"]

    if "net_income" in panel.columns and "assets" in panel.columns:
        panel["roa"] = panel["net_income"] / panel["assets"]

    if "net_income" in panel.columns and "equity" in panel.columns:
        panel["roe"] = panel["net_income"] / panel["equity"]

    if "debt" in panel.columns and "equity" in panel.columns:
        panel["debt_to_equity"] = panel["debt"] / panel["equity"]

    if "current_assets" in panel.columns and "current_liabilities" in panel.columns:
        panel["current_ratio"] = panel["current_assets"] / panel["current_liabilities"]

    if "operating_cash_flow" in panel.columns and "capex" in panel.columns:
        panel["free_cash_flow"] = panel["operating_cash_flow"] - panel["capex"].abs()

    summary = {
        "component": "i2_company_fundamental_panel_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "row_count": int(len(panel)),
        "symbol_count": int(panel["symbol"].nunique()) if not panel.empty else 0,
        "metric_columns": list(panel.columns),
        "status": "i2_company_fundamental_panel_complete",
    }

    panel.to_parquet(out / "i2_company_fundamental_panel_v1.parquet", index=False)
    panel.to_json(out / "i2_company_fundamental_panel_v1.json", orient="records", indent=2)

    with open(out / "i2_company_fundamental_panel_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("I2 Company Fundamental Panel complete")
    print("Rows:", summary["row_count"])
    print("Symbols:", summary["symbol_count"])


if __name__ == "__main__":
    build_i2_company_fundamental_panel_v1()


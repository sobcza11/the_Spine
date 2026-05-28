from pathlib import Path

import pandas as pd


def main():
    repo_root = Path.cwd()

    in_path = repo_root / "data" / "serving" / "c_flow" / "c_flow_serving_v3.parquet"

    if not in_path.exists():
        raise FileNotFoundError(f"Missing C_FLOW serving file: {in_path}")

    df = pd.read_parquet(in_path).copy()

    required_cols = {
        "date",
        "c_flow_score",
        "c_flow_state",
        "c_flow_confidence",
        "rbl_oc",
        "fx_pressure",
        "rates_pressure",
        "macro_pressure",
        "equity_pressure",
        "cot_pressure",
        "commodity_pressure",
        "credit_pressure",
        "fund_flow_pressure",
    }

    missing = required_cols - set(df.columns)
    if missing:
        raise KeyError(f"Missing required columns: {missing}")

    if df.empty:
        raise ValueError("C_FLOW serving file has zero rows.")

    df["date"] = pd.to_datetime(df["date"])

    if df["date"].isna().any():
        raise ValueError("C_FLOW serving file has missing dates.")

    if df["date"].duplicated().any():
        dupes = df.loc[df["date"].duplicated(), "date"].astype(str).tolist()
        raise ValueError(f"Duplicate dates found: {dupes}")

    numeric_cols = [
        "c_flow_score",
        "c_flow_confidence",
        "fx_pressure",
        "rates_pressure",
        "macro_pressure",
        "equity_pressure",
        "cot_pressure",
        "commodity_pressure",
        "credit_pressure",
        "fund_flow_pressure",
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        if df[col].isna().any():
            raise ValueError(f"Numeric column has null/non-numeric values: {col}")

    if not df["c_flow_confidence"].between(0, 1).all():
        raise ValueError("c_flow_confidence must be between 0 and 1.")

    if df["rbl_oc"].isna().any() or (df["rbl_oc"].astype(str).str.len() < 20).any():
        raise ValueError("rbl_oc is missing or too short.")

    latest = df.sort_values("date").iloc[-1]

    print("OK | C_FLOW serving v3 check passed")
    print(f"file: {in_path}")
    print(f"rows: {len(df)}")
    print(f"latest_date: {latest['date'].strftime('%Y-%m-%d')}")
    print(f"c_flow_score: {float(latest['c_flow_score']):.3f}")
    print(f"c_flow_state: {latest['c_flow_state']}")
    print(f"c_flow_confidence: {float(latest['c_flow_confidence']):.3f}")
    print(f"fx_pressure: {float(latest['fx_pressure']):.3f}")
    print(f"rates_pressure: {float(latest['rates_pressure']):.3f}")
    print(f"macro_pressure: {float(latest['macro_pressure']):.3f}")
    print(f"equity_pressure: {float(latest['equity_pressure']):.3f}")
    print(f"cot_pressure: {float(latest['cot_pressure']):.3f}")
    print(f"commodity_pressure: {float(latest['commodity_pressure']):.3f}")
    print(f"credit_pressure: {float(latest['credit_pressure']):.3f}")
    print(f"fund_flow_pressure: {float(latest['fund_flow_pressure']):.3f}")


if __name__ == "__main__":
    main()
    
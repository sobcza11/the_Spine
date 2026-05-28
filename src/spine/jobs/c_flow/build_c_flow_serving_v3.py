from pathlib import Path
import pandas as pd


def read_parquet(path: Path):
    if not path.exists():
        print(f"WARN | missing file: {path}")
        return pd.DataFrame()

    df = pd.read_parquet(path).copy()

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])

    return df.sort_values("date").reset_index(drop=True)


def read_csv(path: Path):
    if not path.exists():
        print(f"WARN | missing file: {path}")
        return pd.DataFrame()

    df = pd.read_csv(path).copy()

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])

    return df.sort_values("date").reset_index(drop=True)


def latest_value(df, col, default=0.0):
    if df.empty or col not in df.columns:
        return default

    try:
        return float(df.iloc[-1][col])
    except Exception:
        return default


def main():
    repo_root = Path.cwd()

    fx = read_parquet(repo_root / "data/serving/fx/fx_serving_v2.parquet")
    rates = read_parquet(repo_root / "data/serving/rates/rates_serving_v2.parquet")
    macro = read_parquet(repo_root / "data/serving/geoscen/geoscen_serving_v2.parquet")
    equities = read_parquet(repo_root / "data/serving/equities/equities_serving_v2.parquet")

    cot = read_csv(
        repo_root / "data/cot/COT_VinV_indexes_1989_2025_monthly.csv"
    )

    wti = read_parquet(
        repo_root / "data/wti/wti_inflation_pressure.parquet"
    )

    credit = read_parquet(
        repo_root / "data/serving/credit/credit_serving_v1.parquet"
    )

    flows = read_parquet(
        repo_root / "data/serving/flows/fund_flow_serving_v1.parquet"
    )

    fx_pressure = latest_value(fx, "tone_direction")
    rates_pressure = latest_value(rates, "dominance_mean")
    macro_pressure = latest_value(macro, "tone_direction")
    equity_pressure = latest_value(equities, "tone_direction")

    cot_pressure = latest_value(cot, "Crypto_Index")
    commodity_pressure = latest_value(wti, "inflation_pressure_z")

    credit_pressure = latest_value(credit, "credit_pressure")
    fund_flow_pressure = latest_value(flows, "fund_flow_pressure")

    components = {
        "fx_pressure": fx_pressure,
        "rates_pressure": rates_pressure,
        "macro_pressure": macro_pressure,
        "equity_pressure": equity_pressure,
        "cot_pressure": cot_pressure,
        "commodity_pressure": commodity_pressure,
        "credit_pressure": credit_pressure,
        "fund_flow_pressure": fund_flow_pressure,
    }

    c_flow_score = sum(components.values()) / len(components)

    if c_flow_score >= 0.30:
        c_flow_state = "Risk-On / Capital Expansion"
    elif c_flow_score <= -0.30:
        c_flow_state = "Risk-Off / Capital Withdrawal"
    else:
        c_flow_state = "Balanced / Monitoring Flow Regime"

    active_components = sum(abs(v) > 0 for v in components.values())
    c_flow_confidence = active_components / len(components)

    rbl_oc = (
        f"C_FLOW reflects active cross-asset capital movement conditions. "
        f"FX={fx_pressure:.3f}, rates={rates_pressure:.3f}, "
        f"macro={macro_pressure:.3f}, equity={equity_pressure:.3f}, "
        f"COT={cot_pressure:.3f}, commodity={commodity_pressure:.3f}, "
        f"credit={credit_pressure:.3f}, flows={fund_flow_pressure:.3f}. "
        f"Overall: C_FLOW indicates {c_flow_state}."
    )

    latest_date = max([
        fx["date"].max(),
        rates["date"].max(),
        macro["date"].max(),
        equities["date"].max(),
    ])

    out = pd.DataFrame([{
        "date": latest_date,
        "c_flow_score": c_flow_score,
        "c_flow_state": c_flow_state,
        "c_flow_confidence": c_flow_confidence,
        "rbl_oc": rbl_oc,
        **components,
    }])

    out_path = repo_root / "data/serving/c_flow/c_flow_serving_v3.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    out.to_parquet(out_path, index=False)

    print("OK | C_FLOW serving v3")
    print(f"output: {out_path}")
    print(out.to_string(index=False))


if __name__ == "__main__":
    main()

    
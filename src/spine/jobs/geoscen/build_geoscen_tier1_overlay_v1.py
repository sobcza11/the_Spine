from pathlib import Path
import pandas as pd


def read_latest(path: Path):
    if not path.exists():
        print(f"WARN | missing: {path}")
        return {}

    df = pd.read_parquet(path).copy()

    if df.empty:
        return {}

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")

    return df.iloc[-1].to_dict()


def fmt(x, digits=3):
    try:
        return round(float(x), digits)
    except Exception:
        return None


def main():
    repo_root = Path.cwd()

    paths = {
        "macro": repo_root / "data/serving/geoscen/geoscen_serving_v2.parquet",
        "services": repo_root / "data/processed/ism/ism_services_regime_summary_v1.parquet",
        "manufacturing": repo_root / "data/processed/ism/ism_industry_regime_summary_v1.parquet",
        "equity_market": repo_root / "data/serving/equities/equity_market_regime_v1.parquet",
        "breadth": repo_root / "data/serving/equities/breadth_factor_serving_v1.parquet",
        "c_flow": repo_root / "data/serving/c_flow/c_flow_serving_v4.parquet",
        "rates": repo_root / "data/serving/rates/rates_serving_v2.parquet",
        "fx": repo_root / "data/serving/fx/fx_serving_v2.parquet",
    }

    latest = {k: read_latest(v) for k, v in paths.items()}

    macro_score = fmt(latest["macro"].get("tone_direction", 0.0))
    services_score = fmt(latest["services"].get("services_regime_score", 0.0))
    manufacturing_score = fmt(latest["manufacturing"].get("ism_regime_score", 0.0))
    equity_score = fmt(latest["equity_market"].get("equity_market_score", 0.0))
    breadth_score = fmt(latest["breadth"].get("breadth_factor_score", 0.0))
    c_flow_score = fmt(latest["c_flow"].get("c_flow_score", 0.0))
    rates_score = fmt(latest["rates"].get("dominance_mean", 0.0))
    fx_score = fmt(latest["fx"].get("tone_direction", 0.0))

    components = {
        "macro_score": macro_score,
        "services_score": services_score,
        "manufacturing_score": manufacturing_score,
        "equity_market_score": equity_score,
        "breadth_factor_score": breadth_score,
        "c_flow_score": c_flow_score,
        "rates_score": rates_score,
        "fx_score": fx_score,
    }

    active = {k: v for k, v in components.items() if v is not None}
    tier1_score = sum(active.values()) / len(active) if active else 0.0
    tier1_confidence = len(active) / len(components)

    if tier1_score >= 0.35:
        tier1_state = "Risk-On / Expansionary Tier 1 Regime"
    elif tier1_score <= -0.35:
        tier1_state = "Risk-Off / Defensive Tier 1 Regime"
    else:
        tier1_state = "Balanced / Monitoring Tier 1 Regime"

    rbl_oc = (
        f"GeoScen Tier 1 overlay reads {tier1_state}. "
        f"Services={services_score}, manufacturing={manufacturing_score}, "
        f"equity market={equity_score}, breadth={breadth_score}, "
        f"C_FLOW={c_flow_score}, rates={rates_score}, FX={fx_score}, macro={macro_score}. "
        f"Current market structure is now supported by real breadth/factor inputs rather than placeholders."
    )

    out = pd.DataFrame([{
        "date": max([
            pd.to_datetime(v.get("date")) for v in latest.values()
            if v and v.get("date") is not None
        ]),
        "tier1_score": tier1_score,
        "tier1_state": tier1_state,
        "tier1_confidence": tier1_confidence,
        "rbl_oc": rbl_oc,
        **components,
        "services_strongest": latest["services"].get("strongest_services"),
        "services_weakest": latest["services"].get("weakest_services"),
        "manufacturing_strongest": latest["manufacturing"].get("strongest_industries"),
        "manufacturing_weakest": latest["manufacturing"].get("weakest_industries"),
        "equity_market_state": latest["equity_market"].get("equity_market_state"),
        "breadth_factor_state": latest["breadth"].get("breadth_factor_state"),
        "c_flow_state": latest["c_flow"].get("c_flow_state"),
    }])

    out_path = repo_root / "data/serving/geoscen/geoscen_tier1_overlay_v1.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(out_path, index=False)

    print("OK | GeoScen Tier 1 overlay v1")
    print(f"output: {out_path}")
    print(out.to_string(index=False))


if __name__ == "__main__":
    main()
    
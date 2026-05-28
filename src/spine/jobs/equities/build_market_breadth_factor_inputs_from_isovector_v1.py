from pathlib import Path
import json

import numpy as np
import pandas as pd


def zscore(s):
    s = pd.to_numeric(s, errors="coerce")
    std = s.std()
    if std == 0 or np.isnan(std):
        return pd.Series([0.0] * len(s), index=s.index)
    return (s - s.mean()) / std


def load_json(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Missing JSON file: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def normalize_price_payload(payload):
    rows = []

    if isinstance(payload, dict):
        for symbol, vals in payload.items():
            if not isinstance(vals, list):
                continue
            for r in vals:
                rows.append({
                    "symbol": str(symbol).upper(),
                    "date": r.get("date") or r.get("as_of_date") or r.get("timestamp"),
                    "close": r.get("close") or r.get("price") or r.get("last") or r.get("value"),
                })

    elif isinstance(payload, list):
        for r in payload:
            rows.append({
                "symbol": str(r.get("symbol") or r.get("ticker") or r.get("etf") or "").upper(),
                "date": r.get("date") or r.get("as_of_date") or r.get("timestamp"),
                "close": r.get("close") or r.get("price") or r.get("last") or r.get("value"),
            })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    return df.dropna(subset=["symbol", "date", "close"]).sort_values(["symbol", "date"])


def pivot_prices(df):
    return (
        df.pivot_table(index="date", columns="symbol", values="close", aggfunc="last")
        .sort_index()
    )


def latest_or_zero(df, col):
    if col not in df.columns:
        return 0.0
    s = pd.to_numeric(df[col], errors="coerce").dropna()
    return float(s.iloc[-1]) if len(s) else 0.0


def main():
    repo_root = Path.cwd()

    src_dir = repo_root / "data" / "serving" / "equities"

    index_json = src_dir / "us_equity_index_data.json"
    sector_json = src_dir / "us_sector_etf_data.json"
    breadth_json = src_dir / "etf_pmi_breadth_by_etf.json"
    sigma_json = src_dir / "equities_sigma_rank.json"

    out_path = (
        repo_root
        / "data"
        / "processed"
        / "equities"
        / "market_breadth_factor_inputs.parquet"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)

    index_payload = load_json(index_json)
    sector_payload = load_json(sector_json)

    index_prices = pivot_prices(normalize_price_payload(index_payload))
    sector_prices = pivot_prices(normalize_price_payload(sector_payload))

    prices = index_prices.join(sector_prices, how="outer").sort_index()

    ret_20d = prices.pct_change(20)

    df = pd.DataFrame(index=prices.index)

    # Market structure proxies
    if {"RSP", "SPY"}.issubset(ret_20d.columns):
        df["equal_weight_vs_cap_weight"] = ret_20d["RSP"] - ret_20d["SPY"]
    elif {"ITOT", "SPY"}.issubset(ret_20d.columns):
        df["equal_weight_vs_cap_weight"] = ret_20d["ITOT"] - ret_20d["SPY"]
    else:
        df["equal_weight_vs_cap_weight"] = 0.0

    if {"QQQ", "SPY"}.issubset(ret_20d.columns):
        df["growth_value"] = ret_20d["QQQ"] - ret_20d["SPY"]
    else:
        df["growth_value"] = 0.0

    if {"IWM", "SPY"}.issubset(ret_20d.columns):
        df["smallcap_vs_market"] = ret_20d["IWM"] - ret_20d["SPY"]
    else:
        df["smallcap_vs_market"] = 0.0

    if {"XLY", "XLP"}.issubset(ret_20d.columns):
        df["cyclical_defensive"] = ret_20d["XLY"] - ret_20d["XLP"]
    else:
        df["cyclical_defensive"] = 0.0

    if {"HYG", "TLT"}.issubset(ret_20d.columns):
        df["credit_risk_appetite"] = ret_20d["HYG"] - ret_20d["TLT"]
    else:
        df["credit_risk_appetite"] = 0.0

    if "SPY" in prices.columns:
        ma50 = prices["SPY"].rolling(50).mean()
        ma200 = prices["SPY"].rolling(200).mean()

        df["pct_above_50dma"] = (prices["SPY"] > ma50).astype(float)
        df["pct_above_200dma"] = (prices["SPY"] > ma200).astype(float)

        df["new_highs_lows_z"] = zscore(
            (prices["SPY"] >= prices["SPY"].rolling(50).max()).astype(float)
        )
    else:
        df["pct_above_50dma"] = 0.0
        df["pct_above_200dma"] = 0.0
        df["new_highs_lows_z"] = 0.0

    # Existing engine expected fields
    df["advance_decline_z"] = zscore(df["equal_weight_vs_cap_weight"])
    df["equal_weight_vs_cap_weight_z"] = zscore(df["equal_weight_vs_cap_weight"])
    df["growth_value_z"] = zscore(df["growth_value"])
    df["cyclical_defensive_z"] = zscore(df["cyclical_defensive"])
    df["momentum_factor_z"] = zscore(ret_20d["SPY"]) if "SPY" in ret_20d.columns else 0.0
    df["quality_factor_z"] = zscore(df["credit_risk_appetite"])

    # Optional: read existing breadth & sigma payloads for diagnostics only.
    try:
        breadth_payload = load_json(breadth_json)
        breadth_rows = len(breadth_payload) if isinstance(breadth_payload, list) else 0
    except Exception:
        breadth_rows = 0

    try:
        sigma_payload = load_json(sigma_json)
        sigma_rows = len(sigma_payload) if isinstance(sigma_payload, list) else 0
    except Exception:
        sigma_rows = 0

    out_cols = [
        "pct_above_50dma",
        "pct_above_200dma",
        "advance_decline_z",
        "new_highs_lows_z",
        "equal_weight_vs_cap_weight_z",
        "growth_value_z",
        "cyclical_defensive_z",
        "momentum_factor_z",
        "quality_factor_z",
    ]

    latest = df[out_cols].dropna(how="all").iloc[-1:].reset_index()
    latest = latest.rename(columns={"index": "date"})

    latest["source_status"] = "real_isovector_json"
    latest["source_note"] = (
        f"Built from IsoVector equities JSON. "
        f"breadth_rows={breadth_rows}; sigma_rows={sigma_rows}"
    )

    latest.to_parquet(out_path, index=False)

    print("OK | market breadth/factor inputs from IsoVector v1")
    print(f"output: {out_path}")
    print(latest.to_string(index=False))


if __name__ == "__main__":
    main()
    
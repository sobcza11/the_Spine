from pathlib import Path
import json
import pandas as pd

ROOT = Path.cwd()

UNIVERSE = ROOT / "data/finstate/inputs/global/finstate_validated_global_universe_v1.parquet"
BALANCE = ROOT / "data/fundamentals/simfin/balance_quarterly.parquet"
CASHFLOW = ROOT / "data/fundamentals/simfin/cashflow_quarterly.parquet"

OUT_DIR = ROOT / "data/serving/finstate"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_JSON = OUT_DIR / "finstate_global_lite_metrics_v1.json"

def safe_num(x, default=0.0):
    try:
        n = float(x)
        return n if pd.notna(n) else default
    except Exception:
        return default

def clamp(x, lo=0, hi=100):
    return max(lo, min(hi, x))

def calc_fragility_pct(debt_eq, fcf_b, cash_b):
    debt_score = clamp((debt_eq / 3.0) * 45)          # high leverage = fragile
    fcf_score = clamp((max(-fcf_b, 0) / 2.0) * 30)    # negative FCF = fragile
    cash_score = clamp((1 / max(cash_b, 0.1)) * 10)   # low cash = fragile

    stabilizer = clamp(max(fcf_b, 0) * 2.0, 0, 20)    # positive FCF reduces fragility

    return clamp(debt_score + fcf_score + cash_score - stabilizer)

u = pd.read_parquet(UNIVERSE)
bs = pd.read_parquet(BALANCE)
cf = pd.read_parquet(CASHFLOW)

for df in [bs, cf]:
    df["Ticker"] = df["Ticker"].astype(str).str.upper()
    df["statement_date"] = pd.to_datetime(df["statement_date"])

u["source_ticker"] = u["source_ticker"].astype(str).str.upper()

bs_latest = bs.sort_values("statement_date").groupby("Ticker", as_index=False).tail(1)
cf_latest = cf.sort_values("statement_date").groupby("Ticker", as_index=False).tail(1)

rows = []

for _, r in u.iterrows():
    src = r["source_ticker"]

    b = bs_latest[bs_latest["Ticker"] == src]
    c = cf_latest[cf_latest["Ticker"] == src]

    if b.empty or c.empty:
        continue

    b = b.iloc[0]
    c = c.iloc[0]

    short_debt = safe_num(b.get("Short Term Debt"))
    long_debt = safe_num(b.get("Long Term Debt"))
    total_debt = short_debt + long_debt

    equity = safe_num(b.get("Total Equity"))
    cash = safe_num(b.get("Cash, Cash Equivalents & Short Term Investments"))

    ocf = safe_num(c.get("Net Cash from Operating Activities"))
    capex = safe_num(c.get("Capital Expenditure"))

    fcf_b = (ocf + capex) / 1e9
    cash_b = cash / 1e9
    debt_eq = total_debt / equity if equity else None

    fragility_pct = calc_fragility_pct(
        debt_eq=debt_eq or 0,
        fcf_b=fcf_b,
        cash_b=cash_b,
    )

    rows.append({
        "ticker": r["ticker"],
        "source_ticker": src,
        "company": r["company"],
        "country": r["country"],
        "region": r["region"],
        "finstate_sector": r["finstate_sector"],
        "as_of_date": str(max(b["statement_date"], c["statement_date"]).date()),
        "fragility_pct_qrt": round(fragility_pct, 2),
        "fcf_b_qrt": round(fcf_b, 3),
        "debt_eq_qrt": round(debt_eq, 3) if debt_eq is not None else None,
        "cash_b_qrt": round(cash_b, 3),
    })

payload = {
    "meta": {
        "name": "finstate_global_lite_metrics_v1",
        "rows": len(rows),
        "source": "SimFin START manual alias bridge",
        "method": "Deterministic fragility from leverage, FCF stress, and cash buffer.",
    },
    "rows": rows,
}

OUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")

print("BUILT:", OUT_JSON)
print("ROWS:", len(rows))
print(pd.DataFrame(rows).to_string(index=False))

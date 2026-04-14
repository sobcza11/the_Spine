import json
from pathlib import Path
import pandas as pd

PRICE_PARQUET = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\spine_us\us_fx_spot_cross_t2.parquet")
SPREADS_PARQUET = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\spine_us\us_fx_10y_spreads.parquet")

OUT_DIR = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine")
PRICE_JSON = OUT_DIR / "fx_price_data.json"
SPREADS_JSON = OUT_DIR / "fx_spreads_data.json"
SIGMA_JSON = OUT_DIR / "fx_sigma_data.json"

SUPPORTED_PRICE_SYMBOLS = {
    "AUDUSD", "EURUSD", "GBPUSD", "USDJPY", "USDCAD", "USDCHF"
}
SUPPORTED_SPREAD_PAIRS = {
    "AUDUSD", "EURUSD", "GBPUSD", "USDJPY", "USDCAD", "USDCHF"
}


def write_json(path: Path, obj) -> None:
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")


if not PRICE_PARQUET.exists():
    raise FileNotFoundError(f"Missing price parquet: {PRICE_PARQUET}")

if not SPREADS_PARQUET.exists():
    raise FileNotFoundError(f"Missing spreads parquet: {SPREADS_PARQUET}")


# =========================
# PRICE EXPORT
# =========================

df_price = pd.read_parquet(PRICE_PARQUET).copy()

required_price = {"symbol", "date", "close"}
missing_price = required_price - set(df_price.columns)
if missing_price:
    raise KeyError(f"PRICE parquet missing required columns: {missing_price}")

df_price["symbol"] = df_price["symbol"].astype(str).str.upper().str.strip()
df_price["date"] = pd.to_datetime(df_price["date"], errors="coerce")
df_price = df_price[df_price["date"].notna()].copy()

for col in ["close", "open", "high", "low"]:
    if col in df_price.columns:
        df_price[col] = pd.to_numeric(df_price[col], errors="coerce")

df_price = df_price[df_price["close"].notna()].copy()

if "open" not in df_price.columns:
    df_price["open"] = df_price["close"]
if "high" not in df_price.columns:
    df_price["high"] = df_price[["open", "close"]].max(axis=1)
if "low" not in df_price.columns:
    df_price["low"] = df_price[["open", "close"]].min(axis=1)

price_payload = {}
for symbol, g in (
    df_price[df_price["symbol"].isin(SUPPORTED_PRICE_SYMBOLS)]
    .sort_values(["symbol", "date"])
    .groupby("symbol", sort=True)
):
    out = g[["date", "open", "high", "low", "close"]].copy()
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")

    price_payload[symbol] = [
        {
            "date": row["date"],
            "open": round(float(row["open"]), 6),
            "high": round(float(row["high"]), 6),
            "low": round(float(row["low"]), 6),
            "close": round(float(row["close"]), 6),
        }
        for _, row in out.iterrows()
    ]

if not price_payload:
    raise ValueError("PRICE export produced zero supported symbols.")

write_json(PRICE_JSON, price_payload)


# =========================
# SPREADS EXPORT
# =========================

df_spreads = pd.read_parquet(SPREADS_PARQUET).copy()

required_spreads = {
    "as_of_date",
    "yld_10y_base",
    "yld_10y_quote",
    "yld_10y_diff",
    "pair",
    "base_ccy",
    "quote_ccy",
}
missing_spreads = required_spreads - set(df_spreads.columns)
if missing_spreads:
    raise KeyError(f"SPREADS parquet missing required columns: {missing_spreads}")

df_spreads["pair"] = df_spreads["pair"].astype(str).str.upper().str.strip()
df_spreads["base_ccy"] = df_spreads["base_ccy"].astype(str).str.upper().str.strip()
df_spreads["quote_ccy"] = df_spreads["quote_ccy"].astype(str).str.upper().str.strip()
df_spreads["as_of_date"] = pd.to_datetime(df_spreads["as_of_date"], errors="coerce")
df_spreads = df_spreads[df_spreads["as_of_date"].notna()].copy()

for col in ["yld_10y_base", "yld_10y_quote", "yld_10y_diff"]:
    df_spreads[col] = pd.to_numeric(df_spreads[col], errors="coerce")

df_spreads = df_spreads[df_spreads["yld_10y_diff"].notna()].copy()

spread_payload = {}
for pair, g in (
    df_spreads[df_spreads["pair"].isin(SUPPORTED_SPREAD_PAIRS)]
    .sort_values(["pair", "as_of_date"])
    .groupby("pair", sort=True)
):
    out = g[
        [
            "as_of_date",
            "yld_10y_diff",
            "yld_10y_base",
            "yld_10y_quote",
            "pair",
            "base_ccy",
            "quote_ccy",
        ]
    ].copy()
    out["as_of_date"] = out["as_of_date"].dt.strftime("%Y-%m-%d")

    spread_payload[pair] = [
        {
            "as_of_date": row["as_of_date"],
            "yld_10y_diff": round(float(row["yld_10y_diff"]), 6),
            "yld_10y_base": round(float(row["yld_10y_base"]), 6),
            "yld_10y_quote": round(float(row["yld_10y_quote"]), 6),
            "pair": row["pair"],
            "base_ccy": row["base_ccy"],
            "quote_ccy": row["quote_ccy"],
        }
        for _, row in out.iterrows()
    ]

if not spread_payload:
    raise ValueError("SPREAD export produced zero supported pairs.")

write_json(SPREADS_JSON, spread_payload)


# =========================
# SIGMA EXPORT
# =========================

sigma_rows = []
for symbol, g in (
    df_price[df_price["symbol"].isin(SUPPORTED_PRICE_SYMBOLS)]
    .sort_values(["symbol", "date"])
    .groupby("symbol", sort=True)
):
    closes = pd.to_numeric(g["close"], errors="coerce").dropna()
    if len(closes) < 21:
        continue

    returns = closes.pct_change().dropna().tail(20)
    if returns.empty:
        continue

    vol = float(returns.std(ddof=0))
    ease = float(1.0 / vol) if vol > 0 else 0.0
    as_of_date = g["date"].max().strftime("%Y-%m-%d")

    sigma_rows.append({
        "pair": f"{symbol[:3]}/{symbol[3:]}",
        "symbol": symbol,
        "vol": vol,
        "ease": ease,
        "as_of_date": as_of_date,
    })

sigma_df = pd.DataFrame(sigma_rows)

if sigma_df.empty:
    raise ValueError("SIGMA export produced zero rows.")

ease_mean = float(sigma_df["ease"].mean())
ease_std = float(sigma_df["ease"].std(ddof=0))
if ease_std == 0:
    ease_std = 1.0

sigma_df["z"] = (sigma_df["ease"] - ease_mean) / ease_std
sigma_df = sigma_df.sort_values("ease", ascending=False).reset_index(drop=True)
sigma_df["rank"] = sigma_df.index + 1

sigma_payload = [
    {
        "pair": row["pair"],
        "symbol": row["symbol"],
        "rank": int(row["rank"]),
        "z": round(float(row["z"]), 6),
        "vol": round(float(row["vol"]), 12),
        "ease": round(float(row["ease"]), 6),
        "as_of_date": row["as_of_date"],
    }
    for _, row in sigma_df[
        ["pair", "symbol", "rank", "z", "vol", "ease", "as_of_date"]
    ].iterrows()
]

write_json(SIGMA_JSON, sigma_payload)

print("Wrote:")
print(PRICE_JSON)
print(SPREADS_JSON)
print(SIGMA_JSON)

print("\nValidation:")
print("price symbols:", sorted(price_payload.keys()))
print("spread pairs :", sorted(spread_payload.keys()))
print("sigma count  :", len(sigma_payload))

required_core = {"AUDUSD", "EURUSD", "GBPUSD", "USDJPY"}
missing_price_core = required_core - set(price_payload.keys())
missing_spread_core = required_core - set(spread_payload.keys())
sigma_symbols = {row["symbol"] for row in sigma_payload}
missing_sigma_core = required_core - sigma_symbols

if missing_price_core:
    raise ValueError(f"Missing core price symbols in export: {sorted(missing_price_core)}")
if missing_spread_core:
    raise ValueError(f"Missing core spread pairs in export: {sorted(missing_spread_core)}")
if missing_sigma_core:
    raise ValueError(f"Missing core sigma symbols in export: {sorted(missing_sigma_core)}")
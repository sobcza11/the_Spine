import os
import json
import requests
import pandas as pd
from datetime import datetime, timezone

OUT_DIR = "dist/fx_depth"
os.makedirs(OUT_DIR, exist_ok=True)

TIINGO_API_KEY = os.getenv("TIINGO_API_KEY")
EIA_API_KEY = os.getenv("EIA_API_KEY")


def get_tiingo_daily(ticker: str, start_date="2015-01-01") -> pd.DataFrame:
    url = f"https://api.tiingo.com/tiingo/daily/{ticker}/prices"
    params = {
        "startDate": start_date,
        "token": TIINGO_API_KEY,
        "format": "json",
    }

    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()

    df = pd.DataFrame(r.json())
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df[["date", "adjClose"]].rename(columns={"adjClose": ticker})
    return df


def get_eia_series(series_id: str, name: str) -> pd.DataFrame:
    url = "https://api.eia.gov/v2/seriesid/"
    params = {
        "api_key": EIA_API_KEY,
        "seriesid": series_id,
    }

    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()

    rows = r.json()["response"]["data"]
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["period"]).dt.date
    df[name] = pd.to_numeric(df["value"], errors="coerce")
    return df[["date", name]].sort_values("date")


def ratio_frame(left: pd.DataFrame, right: pd.DataFrame, left_col: str, right_col: str, metric: str):
    df = left.merge(right, on="date", how="inner")
    df["value"] = df[left_col] / df[right_col]
    df["metric"] = metric
    return df[["date", "metric", "value"]]


def zscore_frame(df: pd.DataFrame, value_col: str, metric: str, window=252):
    out = df.copy()
    out["mean"] = out[value_col].rolling(window).mean()
    out["std"] = out[value_col].rolling(window).std()
    out["value"] = (out[value_col] - out["mean"]) / out["std"]
    out["metric"] = metric
    return out[["date", "metric", "value"]].dropna()


def save_metric(df: pd.DataFrame, filename: str, source: str, frequency: str):
    payload = {
        "meta": {
            "system": "the_Spine",
            "module": "FX_DEPTH",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source": source,
            "frequency": frequency,
            "rows": len(df),
            "as_of": str(df["date"].max()),
        },
        "data": [
            {
                "date": str(row.date),
                "metric": row.metric,
                "value": round(float(row.value), 6),
            }
            for row in df.itertuples(index=False)
        ],
    }

    path = f"{OUT_DIR}/{filename}"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"saved {path}")


# -----------------------------
# 1. FTSE vs SPX
# -----------------------------

ftse = get_tiingo_daily("FTSE")
spx = get_tiingo_daily("SPY")

ftse_spx = ratio_frame(
    ftse,
    spx,
    "FTSE",
    "SPY",
    "FTSE_vs_SPX"
)

save_metric(
    ftse_spx,
    "ftse_vs_spx.json",
    "Tiingo | FTSE proxy + SPY proxy",
    "Daily"
)


# -----------------------------
# 2. WTI vs NatGas
# -----------------------------

wti = get_eia_series("PET.RWTC.D", "WTI")
natgas = get_eia_series("NG.RNGWHHD.D", "NATGAS")

wti_natgas = ratio_frame(
    wti,
    natgas,
    "WTI",
    "NATGAS",
    "WTI_vs_NatGas"
)

save_metric(
    wti_natgas,
    "wti_vs_natgas.json",
    "EIA | WTI Spot + Henry Hub Natural Gas",
    "Daily"
)


# -----------------------------
# 3. Brent Crude
# -----------------------------

brent = get_eia_series("PET.RBRTE.D", "BRENT")

brent_z = zscore_frame(
    brent,
    "BRENT",
    "Brent_Crude_ZScore"
)

save_metric(
    brent_z,
    "brent_crude.json",
    "EIA | Brent Crude Spot",
    "Daily"
)


# -----------------------------
# 4. BCOM vs Nikkei
# -----------------------------

bcom = get_tiingo_daily("BCOM")
nikkei = get_tiingo_daily("EWJ")

bcom_nikkei = ratio_frame(
    bcom,
    nikkei,
    "BCOM",
    "EWJ",
    "BCOM_vs_Nikkei"
)

save_metric(
    bcom_nikkei,
    "bcom_vs_nikkei.json",
    "Tiingo | BCOM proxy + EWJ proxy",
    "Daily"
)


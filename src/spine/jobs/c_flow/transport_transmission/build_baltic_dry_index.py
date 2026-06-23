from pathlib import Path
import json
import pandas as pd


ROOT = Path(__file__).resolve().parents[5]

SERIES_ID = "IGREA"
RAW_URL = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={SERIES_ID}"

OUT_DIR = ROOT / "data" / "c_flow" / "transport_transmission"
SERVING_DIR = ROOT / "data" / "serving" / "cflow"

OUT_PARQUET = OUT_DIR / "baltic_dry_proxy_features.parquet"
OUT_JSON = SERVING_DIR / "baltic_dry_proxy_serving.json"


def classify_state(score):
    if score is None or pd.isna(score):
        return "insufficient_data"

    if score >= 80:
        return "freight_pressure_high"
    if score >= 65:
        return "freight_expansion"
    if score >= 50:
        return "freight_normal"
    if score >= 35:
        return "freight_softening"
    return "freight_contraction"


def safe_pct_change(series, periods):
    return series.pct_change(periods=periods).replace([float("inf"), float("-inf")], pd.NA)


def percentile_score(series, window=252 * 5):
    return (
        series.rolling(window, min_periods=60)
        .rank(pct=True)
        .mul(100)
    )


def build_baltic_dry_proxy():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    SERVING_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(RAW_URL)

    df.columns = [str(c).strip().replace("\ufeff", "") for c in df.columns]

    date_col = df.columns[0]
    value_col = df.columns[1]

    df = df.rename(columns={
        date_col: "date",
        value_col: "baltic_dry_proxy",
    })

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["baltic_dry_proxy"] = pd.to_numeric(df["baltic_dry_proxy"], errors="coerce")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    df = (
        df.dropna(subset=["date", "baltic_dry_proxy"])
        .sort_values("date")
        .reset_index(drop=True)
    )

    df["mom_5d"] = safe_pct_change(df["baltic_dry_proxy"], 5)
    df["mom_20d"] = safe_pct_change(df["baltic_dry_proxy"], 20)
    df["mom_60d"] = safe_pct_change(df["baltic_dry_proxy"], 60)

    df["ma_20d"] = df["baltic_dry_proxy"].rolling(20, min_periods=10).mean()
    df["ma_60d"] = df["baltic_dry_proxy"].rolling(60, min_periods=20).mean()

    df["level_score"] = percentile_score(df["baltic_dry_proxy"])
    df["momentum_score"] = percentile_score(df["mom_20d"])

    df["score"] = (
        (df["level_score"] * 0.65) +
        (df["momentum_score"] * 0.35)
    )

    df["score"] = df["score"].round(3)
    df["state"] = df["score"].apply(classify_state)

    df["source"] = "FRED | Baltic Dry Index"
    df["frequency"] = "Daily"
    df["category"] = "Transport Transmission"
    df["start_year"] = int(df["date"].dt.year.min())

    df.to_parquet(OUT_PARQUET, index=False)

    rows = df.tail(1500).copy()

    rows["date"] = rows["date"].dt.strftime("%Y-%m-%d")

    rows = rows.astype(object).where(
        pd.notna(rows),
        None
    )

    latest = rows.iloc[-1].to_dict()

    latest = rows.iloc[-1].to_dict()

    payload = {
        "meta": {
            "name": "Baltic Dry Index",
            "source": "FRED | IGREA",
            "primary_source": "Kilian Global Real Economic Activity Index",
            "method": "rolling_percentile_score_v1",
            "forecasting": "prohibited",
            "ft_gmi_role": "Transport Transmission",
            "cflow_domain": "Physical Economy",
            "cflow_subsystem": "Logistics / Freight",
            "frequency": "Daily",
            "start_year": int(df["date"].dt.year.min()),
            "revision_risk": "Low",
            "confidence": "High",
        },
        "latest": {
            "date": latest.get("date"),
            "value": latest.get("baltic_dry_proxy"),
            "score": latest.get("score"),
            "state": latest.get("state"),
            "mom_5d": latest.get("mom_5d"),
            "mom_20d": latest.get("mom_20d"),
            "mom_60d": latest.get("mom_60d"),
        },
        "rows": rows.to_dict(orient="records"),
    }

    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(
            payload,
            f,
            indent=2,
            allow_nan=False
        )

    print("OK | Baltic Dry Index built")
    print(OUT_PARQUET)
    print(OUT_JSON)
    print(json.dumps(payload["latest"], indent=2))


if __name__ == "__main__":
    build_baltic_dry_proxy()

from pathlib import Path
import json
import pandas as pd


ROOT = Path(__file__).resolve().parents[5]

INPUT_CSV = ROOT / "data" / "manual" / "container_shipping_index.csv"

OUT_DIR = ROOT / "data" / "c_flow" / "transport_transmission"
SERVING_DIR = ROOT / "data" / "serving" / "cflow"

OUT_PARQUET = OUT_DIR / "container_shipping_index_features.parquet"
OUT_JSON = SERVING_DIR / "container_shipping_index_serving.json"


def classify_state(score):
    if score is None or pd.isna(score):
        return "insufficient_data"
    if score >= 80:
        return "container_freight_pressure_high"
    if score >= 65:
        return "container_freight_expansion"
    if score >= 50:
        return "container_freight_normal"
    if score >= 35:
        return "container_freight_softening"
    return "container_freight_contraction"


def safe_pct_change(series, periods):
    return series.pct_change(periods=periods).replace(
        [float("inf"), float("-inf")],
        pd.NA,
    )


def percentile_score(series, window=252 * 3):
    return series.rolling(window, min_periods=3).rank(pct=True).mul(100)


def build_container_shipping_index():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    SERVING_DIR.mkdir(parents=True, exist_ok=True)

    if not INPUT_CSV.exists():
        raise FileNotFoundError(f"Missing input CSV: {INPUT_CSV}")

    df = pd.read_csv(INPUT_CSV)

    if "date" not in df.columns or "container_shipping_index" not in df.columns:
        raise ValueError("CSV must contain date and container_shipping_index columns.")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["container_shipping_index"] = pd.to_numeric(
        df["container_shipping_index"],
        errors="coerce",
    )

    df = (
        df.dropna(subset=["date", "container_shipping_index"])
        .sort_values("date")
        .reset_index(drop=True)
    )

    df["mom_1p"] = safe_pct_change(df["container_shipping_index"], 1)
    df["mom_4p"] = safe_pct_change(df["container_shipping_index"], 4)
    df["mom_12p"] = safe_pct_change(df["container_shipping_index"], 12)

    df["ma_4p"] = df["container_shipping_index"].rolling(4, min_periods=1).mean()
    df["ma_12p"] = df["container_shipping_index"].rolling(12, min_periods=1).mean()

    df["level_score"] = percentile_score(df["container_shipping_index"])
    df["momentum_score"] = percentile_score(df["mom_4p"])

    df["score"] = (
        df["level_score"].fillna(50) * 0.70
        + df["momentum_score"].fillna(50) * 0.30
    ).round(3)

    df["state"] = df["score"].apply(classify_state)

    df["source"] = "Manual | Container Shipping Index"
    df["frequency"] = "Weekly / Daily"
    df["category"] = "Transport Transmission"
    df["start_year"] = int(df["date"].dt.year.min())

    df.to_parquet(OUT_PARQUET, index=False)

    rows = df.tail(1500).copy()
    rows["date"] = rows["date"].dt.strftime("%Y-%m-%d")

    rows = df.tail(1500).copy()
    rows["date"] = rows["date"].dt.strftime("%Y-%m-%d")
    rows = rows.astype(object).where(pd.notna(rows), None)

    latest = rows.iloc[-1].to_dict()


    payload = {
        "meta": {
            "name": "Container Shipping Index",
            "source": "Manual adapter | FBX/WCI-ready",
            "primary_source": "Freightos FBX or Drewry WCI",
            "method": "rolling_percentile_score_v1",
            "forecasting": "prohibited",
            "ft_gmi_role": "Container Freight Transmission",
            "cflow_domain": "Physical Economy",
            "cflow_subsystem": "Logistics / Freight",
            "frequency": "Weekly / Daily",
            "start_year": int(df["date"].dt.year.min()),
            "revision_risk": "Low",
            "confidence": "Medium until licensed source is automated",
        },
        "latest": {
            "date": latest.get("date"),
            "value": latest.get("container_shipping_index"),
            "score": latest.get("score"),
            "state": latest.get("state"),
            "mom_1p": latest.get("mom_1p"),
            "mom_4p": latest.get("mom_4p"),
            "mom_12p": latest.get("mom_12p"),
        },
        "rows": rows.to_dict(orient="records"),
    }

    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, allow_nan=False)

    print("OK | Container Shipping Index built")
    print(OUT_PARQUET)
    print(OUT_JSON)
    print(json.dumps(payload["latest"], indent=2))


if __name__ == "__main__":
    build_container_shipping_index()
from pathlib import Path
import json
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]

INPUT_JSON = ROOT / "data" / "serving" / "cflow" / "cflow_state_history_serving.json"
OUT_JSON = ROOT / "data" / "serving" / "cflow" / "cflow_composite_serving.json"

DIMENSION_WEIGHTS = {
    "P": 0.22,
    "F": 0.18,
    "L": 0.18,
    "D": 0.05,
    "M": 0.20,
    "X": 0.07,
    "C": 0.10,
}


def classify_transmission_state(score):
    if score is None:
        return "Insufficient Data"

    if score < 1.25:
        return "Contractionary"
    if score < 2.25:
        return "Softening"
    if score < 3.25:
        return "Moderate"
    if score < 4.25:
        return "Expansionary"
    return "High Pressure"


def classify_transmission_bias(row):
    p = row.get("P")
    f = row.get("F")
    l = row.get("L")
    m = row.get("M")
    c = row.get("C")

    if p is None or m is None:
        return "Insufficient Data"

    if p >= 3.5 and l >= 3.5:
        return "Pressure / Liquidity Tightening"

    if m >= 3.25 and p < 3.25:
        return "Expansion Without Excessive Pressure"

    if f >= 3.25 and m <= 2.25:
        return "Fragility Rising"

    if c >= 3.25 and 2.25 <= p <= 3.25:
        return "Coherent Moderate Transmission"

    if m <= 2.0:
        return "Transmission Softening"

    return "Mixed Transmission"


def weighted_score(row):
    total_weight = 0.0
    total_score = 0.0
    active_weights = {}

    for key, weight in DIMENSION_WEIGHTS.items():
        value = row.get(key)

        if value is None or pd.isna(value):
            continue

        total_weight += weight
        total_score += float(value) * weight
        active_weights[key] = weight

    if total_weight == 0:
        return None, active_weights

    return round(total_score / total_weight, 3), active_weights


def load_history():
    if not INPUT_JSON.exists():
        raise FileNotFoundError(f"Missing input: {INPUT_JSON}")

    with open(INPUT_JSON, "r", encoding="utf-8") as f:
        return json.load(f)


def build_cflow_composite():
    payload = load_history()

    rows = payload.get("rows", [])

    if not rows:
        raise ValueError("No rows found in C•FLOW state history payload.")

    composite_rows = []

    for row in rows:
        score, active_weights = weighted_score(row)

        out = {
            "date": row.get("date"),
            "transmission_score": score,
            "transmission_state": classify_transmission_state(score),
            "transmission_bias": classify_transmission_bias(row),
            "active_weights": active_weights,
            "P": row.get("P"),
            "F": row.get("F"),
            "L": row.get("L"),
            "D": row.get("D"),
            "M": row.get("M"),
            "X": row.get("X"),
            "C": row.get("C"),
            "S_proxy": row.get("S_proxy"),
        }

        composite_rows.append(out)

    latest = composite_rows[-1]

    latest_attribution = {
        key: {
            "score": latest.get(key),
            "weight": DIMENSION_WEIGHTS.get(key),
            "weighted_contribution": (
                round(float(latest.get(key)) * DIMENSION_WEIGHTS.get(key), 3)
                if latest.get(key) is not None
                else None
            ),
        }
        for key in DIMENSION_WEIGHTS
    }

    output = {
        "meta": {
            "name": "C•FLOW Economic Transmission Composite",
            "source": "the_Spine | C•FLOW composite engine",
            "method": "weighted_dimension_composite_v1",
            "forecasting": "prohibited",
            "ft_gmi_role": "Economic Transmission Composite",
            "dimension_weights": DIMENSION_WEIGHTS,
        },
        "latest": {
            "date": latest.get("date"),
            "transmission_score": latest.get("transmission_score"),
            "transmission_state": latest.get("transmission_state"),
            "transmission_bias": latest.get("transmission_bias"),
            "S_proxy": latest.get("S_proxy"),
            "attribution": latest_attribution,
        },
        "rows": composite_rows,
    }

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)

    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    print("OK | C•FLOW Composite built")
    print(OUT_JSON)
    print(json.dumps(output["latest"], indent=2))


if __name__ == "__main__":
    build_cflow_composite()
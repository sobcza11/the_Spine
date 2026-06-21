from pathlib import Path
import json
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]

INPUT_DIR = ROOT / "data" / "serving" / "c_flow"
OUT_DIR = ROOT / "data" / "serving" / "cflow"

OUT_JSON = OUT_DIR / "cflow_state_engine_serving.json"

STATE_FILES = {
    "weekly_economic_index": "weekly_economic_index_serving.json",
    "core_pce": "core_pce_serving.json",
    "industrial_production": "industrial_production_serving.json",
    "building_permits": "building_permits_serving.json",
    "jolts_openings": "jolts_openings_serving.json",
    "initial_jobless_claims": "initial_jobless_claims_serving.json",
    "weekly_hours_worked": "weekly_hours_worked_serving.json",
    "capacity_utilization": "capacity_utilization_serving.json",
    "retail_sales": "retail_sales_serving.json",
    "consumer_sentiment": "consumer_sentiment_serving.json",
    "core_cpi": "core_cpi_serving.json",
    "ppi_finished_goods": "ppi_finished_goods_serving.json",
    "real_personal_income": "real_personal_income_serving.json",
}


MAPPING = {
    "P": [
        "weekly_economic_index",
        "core_pce",
        "industrial_production",
        "building_permits",
        "jolts_openings",
        "initial_jobless_claims",
        "capacity_utilization",
        "retail_sales",
        "core_cpi",
        "ppi_finished_goods",
        "real_personal_income",
    ],
    "F": [
        "core_pce",
        "building_permits",
        "jolts_openings",
        "initial_jobless_claims",
        "weekly_hours_worked",
        "core_cpi",
        "real_personal_income",
    ],
    "L": [
        "core_pce",
        "core_cpi",
    ],
    "D": [],
    "M": [
        "weekly_economic_index",
        "industrial_production",
        "building_permits",
        "jolts_openings",
        "initial_jobless_claims",
        "weekly_hours_worked",
        "capacity_utilization",
        "retail_sales",
        "consumer_sentiment",
        "ppi_finished_goods",
        "real_personal_income",
    ],
    "X": [],
    "C": [
        "weekly_economic_index",
        "core_pce",
        "industrial_production",
        "jolts_openings",
        "weekly_hours_worked",
        "capacity_utilization",
        "core_cpi",
        "ppi_finished_goods",
        "real_personal_income",
    ],
}


STATE_LABELS = {
    "P": ["Very Low", "Low", "Neutral", "Elevated", "High", "Extreme"],
    "F": ["Very Robust", "Robust", "Stable", "Vulnerable", "Fragile", "Highly Fragile"],
    "L": ["Abundant", "Easy", "Normal", "Tightening", "Tight", "Constrained"],
    "D": ["Unified", "Aligned", "Mixed", "Diverging", "Fragmented", "Extreme Divergence"],
    "M": ["Sharp Contraction", "Contraction", "Flat", "Expansion", "Strong Expansion", "Exceptional Expansion"],
    "X": ["None", "Minimal", "Normal", "Elevated", "Significant", "Systemic"],
    "C": ["Chaotic", "Weak", "Mixed", "Moderate", "Strong", "Exceptional"],
}


def clamp(value, low=0.0, high=5.0):
    return max(low, min(high, value))


def score_from_signal(value, neutral=0.0, scale=2.0, invert=False):
    if value is None or pd.isna(value):
        return None

    raw = float(value)
    if invert:
        raw = -raw

    return clamp(2.5 + ((raw - neutral) / scale))


def load_payload(key):
    path = INPUT_DIR / STATE_FILES[key]

    if not path.exists():
        raise FileNotFoundError(f"Missing input: {path}")

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def extract_indicator_score(key, payload):
    latest = payload.get("latest", {})
    meta = payload.get("meta", {})

    pressure = latest.get("pressure_score")
    fragility = latest.get("fragility_score")
    momentum = latest.get("momentum_score")
    coherence = latest.get("coherence_score")

    return {
        "key": key,
        "name": meta.get("name", key),
        "date": latest.get("date") or meta.get("as_of_date"),
        "state": latest.get("state"),
        "scores": {
            "P": score_from_signal(pressure, neutral=0, scale=2),
            "F": score_from_signal(fragility, neutral=0, scale=2),
            "L": score_from_signal(pressure, neutral=0, scale=2),
            "D": None,
            "M": score_from_signal(momentum, neutral=0, scale=2),
            "X": None,
            "C": score_from_signal(coherence, neutral=0, scale=2),
        },
    }


def classify_state(letter, score):
    if score is None:
        return "Insufficient Data"

    idx = int(round(clamp(score, 0, 5)))
    return STATE_LABELS[letter][idx]


def build_dimension(letter, indicators):
    contributors = []

    for key in MAPPING[letter]:
        item = indicators.get(key)

        if not item:
            continue

        score = item["scores"].get(letter)

        if score is None:
            continue

        contributors.append({
            "key": key,
            "name": item["name"],
            "score": round(float(score), 3),
            "state": item["state"],
            "date": item["date"],
        })

    if not contributors:
        return {
            "score": None,
            "state": "Insufficient Data",
            "contributors": [],
            "top_positive": [],
            "top_negative": [],
        }

    contributors = sorted(
        contributors,
        key=lambda x: x["score"],
        reverse=True,
    )

    score = (
        sum(c["score"] for c in contributors)
        / len(contributors)
    )

    top_positive = contributors[:3]

    top_negative = sorted(
        contributors,
        key=lambda x: x["score"]
    )[:3]

    return {
        "score": round(float(score), 3),
        "state": classify_state(letter, score),
        "contributors": contributors,
        "top_positive": top_positive,
        "top_negative": top_negative,
        "contributor_count": len(contributors),
    }


def build_cflow_state_engine():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    indicators = {}

    for key in STATE_FILES:
        payload = load_payload(key)
        indicators[key] = extract_indicator_score(key, payload)

    dimensions = {
        letter: build_dimension(letter, indicators)
        for letter in ["P", "F", "L", "D", "M", "X", "C"]
    }

    active_scores = [
        dim["score"]
        for dim in dimensions.values()
        if dim["score"] is not None
    ]

    systemicity = (
        round(sum(active_scores) / len(active_scores), 3)
        if active_scores
        else None
    )


    attribution_summary = {}

    for letter, dimension in dimensions.items():

        attribution_summary[letter] = {
            "score": dimension["score"],
            "state": dimension["state"],
            "top_positive": [
                x["name"]
                for x in dimension.get("top_positive", [])
            ],
            "top_negative": [
                x["name"]
                for x in dimension.get("top_negative", [])
            ],
        }


    payload = {
        "meta": {
            "name": "C•FLOW State Engine",
            "source": "the_Spine | C•FLOW deterministic state engine",
            "method": "bounded_average_v1",
            "forecasting": "prohibited",
            "ft_gmi_role": "State Mapping",
            "iv_mapping": ["P", "F", "L", "D", "M", "X", "C", "S"],
        },
        "latest": {
            "systemicity_proxy": systemicity,
            "systemicity_state": classify_state("C", systemicity) if systemicity is not None else "Insufficient Data",
        },
        "attribution": attribution_summary,
        "dimensions": dimensions,
        "indicators": indicators,
    }

    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print("OK | C•FLOW State Engine built")
    print(OUT_JSON)
    print(json.dumps(payload["latest"], indent=2))


if __name__ == "__main__":
    build_cflow_state_engine()
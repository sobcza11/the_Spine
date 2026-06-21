from pathlib import Path
import json
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]

INPUT_DIR = ROOT / "data" / "serving" / "c_flow"
OUT_DIR = ROOT / "data" / "serving" / "cflow"

OUT_JSON = OUT_DIR / "cflow_state_history_serving.json"

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
    "L": ["core_pce", "core_cpi"],
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


def score_from_signal(value, neutral=0.0, scale=2.0):
    if value is None or pd.isna(value):
        return None

    return clamp(2.5 + ((float(value) - neutral) / scale))


def classify_state(letter, score):
    if score is None:
        return "Insufficient Data"

    idx = int(round(clamp(score, 0, 5)))
    return STATE_LABELS[letter][idx]


def load_payload(key):
    path = INPUT_DIR / STATE_FILES[key]

    if not path.exists():
        raise FileNotFoundError(f"Missing input: {path}")

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def score_row(row):
    return {
        "P": score_from_signal(row.get("pressure_score")),
        "F": score_from_signal(row.get("fragility_score")),
        "L": score_from_signal(row.get("pressure_score")),
        "D": None,
        "M": score_from_signal(row.get("momentum_score")),
        "X": None,
        "C": score_from_signal(row.get("coherence_score")),
    }


def build_indicator_history():
    histories = {}

    for key in STATE_FILES:
        payload = load_payload(key)
        meta = payload.get("meta", {})
        rows = payload.get("rows", [])

        df = pd.DataFrame(rows)

        if df.empty or "date" not in df.columns:
            histories[key] = {
                "name": meta.get("name", key),
                "rows": pd.DataFrame(),
            }
            continue

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

        scored_rows = []

        for _, row in df.iterrows():
            row_dict = row.to_dict()
            scores = score_row(row_dict)

            scored_rows.append({
                "date": row["date"],
                "key": key,
                "name": meta.get("name", key),
                "state": row_dict.get("state"),
                "scores": scores,
            })

        histories[key] = {
            "name": meta.get("name", key),
            "rows": pd.DataFrame(scored_rows),
        }

    return histories


def latest_score_on_or_before(history_df, target_date, key, letter):
    if history_df.empty:
        return None

    scoped = history_df[history_df["date"] <= target_date]

    if scoped.empty:
        return None

    latest = scoped.iloc[-1]
    score = latest["scores"].get(letter)

    if score is None:
        return None

    return {
        "key": key,
        "name": latest["name"],
        "score": round(float(score), 3),
        "state": latest["state"],
        "date": latest["date"].strftime("%Y-%m-%d"),
    }


def build_dimension_for_date(letter, histories, target_date):
    contributors = []

    for key in MAPPING[letter]:
        history_df = histories[key]["rows"]
        item = latest_score_on_or_before(history_df, target_date, key, letter)

        if item is not None:
            contributors.append(item)

    if not contributors:
        return {
            "score": None,
            "state": "Insufficient Data",
            "contributors": [],
        }

    score = sum(x["score"] for x in contributors) / len(contributors)

    return {
        "score": round(float(score), 3),
        "state": classify_state(letter, score),
        "contributors": contributors,
    }


def build_cflow_state_history():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    histories = build_indicator_history()

    all_dates = sorted({
        row["date"]
        for history in histories.values()
        for _, row in history["rows"].iterrows()
    })

    history_rows = []

    for dt in all_dates:
        dimensions = {
            letter: build_dimension_for_date(letter, histories, dt)
            for letter in ["P", "F", "L", "D", "M", "X", "C"]
        }

        active_scores = [
            dim["score"]
            for dim in dimensions.values()
            if dim["score"] is not None
        ]

        systemicity_proxy = (
            round(sum(active_scores) / len(active_scores), 3)
            if active_scores
            else None
        )

        row = {
            "date": dt.strftime("%Y-%m-%d"),
            "P": dimensions["P"]["score"],
            "P_state": dimensions["P"]["state"],
            "F": dimensions["F"]["score"],
            "F_state": dimensions["F"]["state"],
            "L": dimensions["L"]["score"],
            "L_state": dimensions["L"]["state"],
            "D": dimensions["D"]["score"],
            "D_state": dimensions["D"]["state"],
            "M": dimensions["M"]["score"],
            "M_state": dimensions["M"]["state"],
            "X": dimensions["X"]["score"],
            "X_state": dimensions["X"]["state"],
            "C": dimensions["C"]["score"],
            "C_state": dimensions["C"]["state"],
            "S_proxy": systemicity_proxy,
            "S_state": classify_state("C", systemicity_proxy)
            if systemicity_proxy is not None
            else "Insufficient Data",
        }

        history_rows.append(row)

    latest = history_rows[-1] if history_rows else {}

    payload = {
        "meta": {
            "name": "C•FLOW Historical State Series",
            "source": "the_Spine | C•FLOW deterministic state history engine",
            "method": "asof_bounded_average_v1",
            "forecasting": "prohibited",
            "ft_gmi_role": "Historical State Series",
            "iv_mapping": ["P", "F", "L", "D", "M", "X", "C", "S"],
            "row_count": len(history_rows),
        },
        "latest": latest,
        "rows": history_rows,
    }

    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print("OK | C•FLOW State History built")
    print(OUT_JSON)
    print(json.dumps(payload["latest"], indent=2))


if __name__ == "__main__":
    build_cflow_state_history()
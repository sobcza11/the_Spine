import json
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict

ROOT = Path(__file__).resolve().parents[3]

INPUTS = {
    "global_region_latest": (
        ROOT
        / "data"
        / "serving"
        / "equities"
        / "global_equity_region_latest.json"
    ),
    "industry_panel": (
        ROOT
        / "data"
        / "serving"
        / "equities"
        / "industry_panel_serving.json"
    ),
}

OUTPUT = (
    ROOT
    / "data"
    / "serving"
    / "oraclechambers"
    / "equities_chamber_serving.json"
)


def load_json(path):
    if not path.exists():
        raise FileNotFoundError(f"Missing input: {path}")

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def clamp(value, low=0.0, high=100.0):
    return max(low, min(high, value))


def z_to_stress_score(value):
    if value is None:
        return None

    return round(
        clamp(50.0 - float(value) * 15.0),
        2
    )


def state_from_score(score):
    if score >= 75:
        return "Stress"
    if score >= 50:
        return "Watch"
    if score >= 25:
        return "Soft"
    return "Stable"


def latest_industry_rows(rows):
    if not rows:
        return []

    latest_date = max(
        r.get("date")
        for r in rows
        if r.get("date")
    )

    return [
        r for r in rows
        if r.get("date") == latest_date
    ]


def average(values):
    values = [
        v for v in values
        if v is not None
    ]

    if not values:
        return None

    return sum(values) / len(values)


def main():

    global_region = load_json(INPUTS["global_region_latest"])
    industry_panel = load_json(INPUTS["industry_panel"])

    regions = global_region.get("regions", [])
    rows = global_region.get("rows", [])

    region_scores = [
        z_to_stress_score(r.get("score"))
        for r in regions
    ]

    regional_stress_score = round(
        average(region_scores),
        2
    )

    weak_regions = [
        r for r in regions
        if "Risk-Off" in str(r.get("state", ""))
    ]

    regional_breadth_score = round(
        clamp(
            (len(weak_regions) / max(len(regions), 1)) * 100
        ),
        2
    )

    row_stress_scores = [
        z_to_stress_score(r.get("equity_region_score"))
        for r in rows
    ]

    global_momentum_score = round(
        average(row_stress_scores),
        2
    )

    latest_industries = latest_industry_rows(
        industry_panel
    )

    industry_sig_scores = [
        z_to_stress_score(r.get("Sig"))
        for r in latest_industries
    ]

    industry_dispersion_score = round(
        average(industry_sig_scores),
        2
    )

    industry_count = len(latest_industries)

    components = {
        "Regional Stress":
            regional_stress_score,
        "Regional Breadth Weakness":
            regional_breadth_score,
        "Global Equity Momentum":
            global_momentum_score,
        "Industry Dispersion":
            industry_dispersion_score,
    }

    valid_components = {
        k: v for k, v in components.items()
        if v is not None
    }

    chamber_score = round(
        sum(valid_components.values())
        / len(valid_components),
        2
    )

    ranked = sorted(
        valid_components.items(),
        key=lambda x: x[1],
        reverse=True
    )

    state = state_from_score(chamber_score)

    payload = {
        "metric": "Equities Chamber",
        "category": "Oracle Chambers",
        "sub_category": "Equities",
        "source": "the_Spine",
        "frequency": "Mixed",

        "meta": {
            "generated_at":
                datetime.now(timezone.utc).isoformat(),
            "forecasting": False,
            "prediction": False,
            "trade_recommendation": False,
            "chamber_version": "1.0",
            "inputs": {
                "global_region_latest":
                    str(INPUTS["global_region_latest"]),
                "industry_panel":
                    str(INPUTS["industry_panel"]),
            }
        },

        "latest": {
            "date":
                rows[0].get("date")
                if rows else None,
            "score": chamber_score,
            "state": state,
            "region_count": len(regions),
            "weak_region_count": len(weak_regions),
            "industry_count": industry_count,
        },

        "observation":
            f"Equities currently classify as "
            f"{state} with a diagnostic "
            f"score of {chamber_score}.",

        "measurement": [
            {
                "component": name,
                "score": score
            }
            for name, score in ranked
        ],

        "diagnosis":
            f"{ranked[0][0]} is the largest "
            f"active equities contributor.",

        "attribution": {
            "drivers": [
                {
                    "component": name,
                    "score": score
                }
                for name, score in ranked[:2]
            ],
            "offsets": [
                {
                    "component": name,
                    "score": score
                }
                for name, score in ranked[-2:]
            ],
            "regions": regions,
            "weak_regions": weak_regions[:5],
        },

        "governance": {
            "observe": True,
            "measure": True,
            "diagnose": True,
            "attribute": True,
            "forecast": False,
            "predict": False,
            "recommend_trades": False
        }
    }

    OUTPUT.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with open(
        OUTPUT,
        "w",
        encoding="utf-8"
    ) as f:
        json.dump(payload, f, indent=2)

    print(f"Wrote {OUTPUT}")


if __name__ == "__main__":
    main()
    
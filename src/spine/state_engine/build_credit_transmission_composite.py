from pathlib import Path
import json
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]

INPUT = ROOT / "data/serving/c_flow/high_yield_oas_serving.json"
OUTPUT = ROOT / "data/serving/c_flow/credit_transmission_composite_serving.json"


def classify(score: float) -> str:
    if score < 25:
        return "Benign"
    if score < 50:
        return "Watch"
    if score < 75:
        return "Stress"
    return "Constraint"


def main():
    payload = json.loads(INPUT.read_text())
    rows = payload["rows"]

    df = pd.DataFrame(rows)
    latest = df.sort_values("date").iloc[-1]

    hy_oas_value = float(latest["hy_oas"])
    hy_oas_z = float(latest["hy_oas_z"])

    # Convert z-score into bounded 0–100 diagnostic score.
    credit_score = round(min(max((hy_oas_z + 3) / 6 * 100, 0), 100), 2)

    out = {
        "metric": "Credit Transmission Composite",
        "category": "C•FLOW",
        "sub_category": "Credit",
        "iv_category": ["F", "C", "S"],
        "source": "the_Spine | C•FLOW credit component",
        "frequency": "Daily",

        "meta": {
            "name": "Credit Transmission Composite",
            "ft_gmi_role": "Financial Transmission",
            "forecasting": "prohibited",
            "phase": "8B",
        },
        "latest": {
            "date": str(latest["date"]),
            "value": credit_score,
            "score": credit_score,
            "state": classify(credit_score),
            "bias": "diagnostic_only",
        },
        "attribution": {
            "high_yield_oas": hy_oas_value,
            "high_yield_oas_z": hy_oas_z,
            "version_note": "v1 uses HY OAS only; IG OAS and spread regime pending.",
        },
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(out, indent=2))

    print(f"OK | wrote {OUTPUT}")


if __name__ == "__main__":
    main()

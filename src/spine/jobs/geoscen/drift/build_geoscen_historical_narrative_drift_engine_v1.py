from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd


REPO_ROOT = Path.cwd()

OUT_DIR = REPO_ROOT / "data" / "serving" / "geoscen" / "drift"
OUT_DIR.mkdir(parents=True, exist_ok=True)

SOURCE_CONTRACT = {
    "rbl_reports": REPO_ROOT / "data" / "geoscen" / "llm" / "geoscen_rbl_report_v1.parquet",
    "cb_cognition": REPO_ROOT / "data" / "serving" / "geoscen" / "cb" / "geoscen_cross_country_policy_cognition_v1.json",
    "contradiction": REPO_ROOT / "data" / "serving" / "geoscen" / "contradiction" / "geoscen_contradiction_engine_v1.json",
}


DRIFT_TERMS = {
    "growth": ["growth", "demand", "slowdown", "weakening", "expansion"],
    "inflation": ["inflation", "prices", "disinflation", "commodity pressure"],
    "policy": ["hawkish", "dovish", "policy", "rates", "tightening", "easing"],
    "risk": ["risk-on", "risk-off", "stress", "defensive", "volatility"],
    "uncertainty": ["uncertainty", "uncertain", "monitoring", "divergence"],
}


def read_json(path: Path) -> dict:
    if not path.exists():
        return {"available": False, "path": str(path)}
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    if isinstance(obj, dict):
        obj["available"] = True
    return obj


def count_terms(text: str, terms: list[str]) -> int:
    text = str(text).lower()
    return sum(text.count(term.lower()) for term in terms)


def classify_drift(delta: float) -> str:
    if delta >= 2:
        return "accelerating"
    if delta <= -2:
        return "decelerating"
    if delta != 0:
        return "mild_shift"
    return "stable"


def main() -> None:
    rbl_path = SOURCE_CONTRACT["rbl_reports"]

    if not rbl_path.exists():
        raise FileNotFoundError(f"Missing RBL report file: {rbl_path}")

    df = pd.read_parquet(rbl_path).copy()

    required = {"date", "rbl_report"}
    missing = required - set(df.columns)
    if missing:
        raise KeyError(f"RBL report missing required cols: {missing}")

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    for family, terms in DRIFT_TERMS.items():
        df[f"{family}_term_count"] = df["rbl_report"].apply(lambda x: count_terms(x, terms))
        df[f"{family}_term_delta"] = df[f"{family}_term_count"].diff().fillna(0)
        df[f"{family}_drift_state"] = df[f"{family}_term_delta"].apply(classify_drift)

    latest = df.tail(1).iloc[0].to_dict()

    drift_rows = []
    for family in DRIFT_TERMS:
        drift_rows.append({
            "signal_family": family,
            "term_count": int(latest.get(f"{family}_term_count", 0)),
            "term_delta": float(latest.get(f"{family}_term_delta", 0)),
            "drift_state": latest.get(f"{family}_drift_state", "stable"),
        })

    active_drift = [
        row for row in drift_rows
        if row["drift_state"] != "stable"
    ]

    cb_cognition = read_json(SOURCE_CONTRACT["cb_cognition"])
    contradiction = read_json(SOURCE_CONTRACT["contradiction"])

    drift_score = round(
        min(
            1.0,
            sum(abs(row["term_delta"]) for row in drift_rows) / 20.0
        ),
        4,
    )

    if drift_score >= 0.70:
        drift_regime = "High Narrative Drift"
    elif drift_score >= 0.35:
        drift_regime = "Moderate Narrative Drift"
    elif drift_score > 0:
        drift_regime = "Low Narrative Drift"
    else:
        drift_regime = "Stable Narrative Regime"

    payload = {
        "component": "GeoScen Historical Narrative Drift Engine",
        "version": "v1",
        "built_at_utc": datetime.now(timezone.utc).isoformat(),
        "latest_date": str(latest.get("date")),
        "drift_score": drift_score,
        "drift_regime": drift_regime,
        "active_drift_count": len(active_drift),
        "drift_rows": drift_rows,
        "active_drift": active_drift,
        "cb_cognition_available": cb_cognition.get("available"),
        "contradiction_available": contradiction.get("available"),
        "oraclechambers_ready": True,
        "governance": {
            "rules_based": True,
            "ai_last": True,
            "explainable": True,
            "historical_drift_ready": True,
            "source_provenance_required": True,
        },
    }

    out_json = OUT_DIR / "geoscen_historical_narrative_drift_engine_v1.json"
    out_txt = OUT_DIR / "geoscen_historical_narrative_drift_engine_v1.txt"
    out_parquet = OUT_DIR / "geoscen_historical_narrative_drift_panel_v1.parquet"

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    df.to_parquet(out_parquet, index=False)

    with open(out_txt, "w", encoding="utf-8-sig") as f:
        f.write("GEOSCEN HISTORICAL NARRATIVE DRIFT ENGINE V1\n")
        f.write("=" * 60 + "\n\n")
        f.write(f"latest_date: {payload['latest_date']}\n")
        f.write(f"drift_score: {payload['drift_score']}\n")
        f.write(f"drift_regime: {payload['drift_regime']}\n")
        f.write(f"active_drift_count: {payload['active_drift_count']}\n\n")

        f.write("DRIFT ROWS\n")
        f.write("-" * 60 + "\n")
        for row in drift_rows:
            f.write(
                f"- {row['signal_family']} | "
                f"count={row['term_count']} | "
                f"delta={row['term_delta']} | "
                f"state={row['drift_state']}\n"
            )

        f.write("\nACTIVE DRIFT\n")
        f.write("-" * 60 + "\n")
        if active_drift:
            for row in active_drift:
                f.write(
                    f"- {row['signal_family']} | "
                    f"{row['drift_state']} | "
                    f"delta={row['term_delta']}\n"
                )
        else:
            f.write("- None detected\n")

    print("OK | GeoScen Historical Narrative Drift Engine v1 built")
    print(f"drift_score        : {payload['drift_score']}")
    print(f"drift_regime       : {payload['drift_regime']}")
    print(f"active_drift_count : {payload['active_drift_count']}")
    print("\nArtifacts written:")
    print(out_json)
    print(out_txt)
    print(out_parquet)


if __name__ == "__main__":
    main()

    
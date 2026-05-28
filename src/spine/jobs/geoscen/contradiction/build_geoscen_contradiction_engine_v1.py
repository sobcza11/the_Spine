from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd


REPO_ROOT = Path.cwd()

OUT_DIR = REPO_ROOT / "data" / "serving" / "geoscen" / "contradiction"
OUT_DIR.mkdir(parents=True, exist_ok=True)

SOURCE_CONTRACT = {
    "frontend": REPO_ROOT / "data" / "serving" / "geoscen" / "frontend" / "geoscen_frontend_intelligence_layer_v1.json",
    "rbl": REPO_ROOT / "data" / "serving" / "geoscen" / "geoscen_rbl_synthesis_v1.json",
    "cb": REPO_ROOT / "data" / "geoscen" / "signals" / "macro_cb_oc_signals_v1.parquet",
    "c_flow": REPO_ROOT / "data" / "serving" / "c_flow" / "c_flow_latest_v5.json",
    "breadth": REPO_ROOT / "data" / "serving" / "equities" / "breadth_factor_serving_v1.parquet",
}


def read_json(path: Path) -> dict:
    if not path.exists():
        return {"available": False, "path": str(path)}
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    if isinstance(obj, list):
        obj = obj[-1] if obj else {}
    if isinstance(obj, dict):
        obj["available"] = True
    return obj


def read_latest_parquet(path: Path) -> dict:
    if not path.exists():
        return {"available": False, "path": str(path)}
    df = pd.read_parquet(path)
    if df.empty:
        return {"available": False, "path": str(path), "reason": "empty"}
    row = df.sort_values(df.columns[0]).tail(1).iloc[0].to_dict()
    row["available"] = True
    return row


def text_has(text: str, terms: list[str]) -> bool:
    text = text.lower()
    return any(term.lower() in text for term in terms)


def score_rule(trigger: bool, base: float, persistence: float = 0.0) -> float:
    if not trigger:
        return 0.0
    return round(min(1.0, base + persistence), 4)


def severity(score: float) -> str:
    if score >= 0.75:
        return "high"
    if score >= 0.45:
        return "medium"
    if score > 0:
        return "low"
    return "none"


def build_contradiction_rules(rbl: dict, cb: dict, c_flow: dict, breadth: dict) -> list[dict]:
    rbl_text = str(rbl.get("rbl", "")).lower()

    policy_hawkish = (
        text_has(rbl_text, ["hawkish", "restrictive policy", "tightening"])
        or float(cb.get("policy_tone", 0) or 0) > 40
        or float(cb.get("policy_tone_score", 0) or 0) > 40
    )

    policy_dovish = (
        text_has(rbl_text, ["dovish", "easing"])
        or float(cb.get("policy_tone", 0) or 0) < -20
        or float(cb.get("policy_tone_score", 0) or 0) < -20
    )

    weakening_pmi = text_has(
        rbl_text,
        ["weakening manufacturing", "falling pmi", "weak pmi", "industrial deterioration"],
    )

    risk_on = text_has(rbl_text, ["risk-on", "healthy breadth", "equity participation remains anchored"])

    defensive_flow = text_has(
        str(c_flow).lower() + " " + rbl_text,
        ["defensive", "risk-off", "flow pressure", "capital-flow pressure"],
    )

    weak_breadth = (
        text_has(rbl_text, ["weak breadth", "breadth deterioration"])
        or float(breadth.get("breadth_factor_score", 1) or 1) < 0.40
    )

    commodity_pressure = text_has(
        rbl_text,
        ["commodity pressure", "wti pressure", "oil pressure", "inflation pressure"],
    )

    strong_services = text_has(rbl_text, ["strong services expansion", "services resilience"])
    weak_manufacturing = text_has(rbl_text, ["weakening manufacturing", "manufacturing regime"])

    rules = [
        {
            "id": "hawkish_policy_weak_growth",
            "title": "Hawkish Policy / Weak Growth",
            "trigger": policy_hawkish and weakening_pmi,
            "score": score_rule(policy_hawkish and weakening_pmi, 0.70),
            "evidence": ["CB tone", "PMI / ISM qualitative layer", "RBL"],
            "rbl_sentence": "Policy tone appears restrictive while manufacturing or growth evidence is weakening.",
        },
        {
            "id": "risk_on_defensive_flow",
            "title": "Risk-On Equities / Defensive Flow",
            "trigger": risk_on and defensive_flow,
            "score": score_rule(risk_on and defensive_flow, 0.55),
            "evidence": ["Breadth", "C_FLOW", "RBL"],
            "rbl_sentence": "Equity participation remains constructive while capital-flow evidence shows defensive pressure.",
        },
        {
            "id": "risk_on_weak_breadth",
            "title": "Risk-On Narrative / Weak Breadth",
            "trigger": risk_on and weak_breadth,
            "score": score_rule(risk_on and weak_breadth, 0.60),
            "evidence": ["Breadth factor", "RBL"],
            "rbl_sentence": "Risk-on interpretation conflicts with weakening market breadth.",
        },
        {
            "id": "services_manufacturing_split",
            "title": "Strong Services / Weak Manufacturing",
            "trigger": strong_services and weak_manufacturing,
            "score": score_rule(strong_services and weak_manufacturing, 0.50),
            "evidence": ["ISM services", "ISM manufacturing", "PMI qualitative layer"],
            "rbl_sentence": "Services strength is masking weaker manufacturing conditions.",
        },
        {
            "id": "commodity_pressure_dovish_policy",
            "title": "Commodity Pressure / Dovish Policy",
            "trigger": commodity_pressure and policy_dovish,
            "score": score_rule(commodity_pressure and policy_dovish, 0.65),
            "evidence": ["WTI", "CB tone", "RBL"],
            "rbl_sentence": "Commodity-linked inflation pressure conflicts with dovish policy tone.",
        },
    ]

    return [
        {
            **rule,
            "severity": severity(rule["score"]),
        }
        for rule in rules
        if rule["trigger"]
    ]


def build_divergence_matrix(active_rules: list[dict]) -> list[dict]:
    return [
        {
            "rule_id": rule["id"],
            "axis": rule["title"],
            "severity": rule["severity"],
            "score": rule["score"],
            "evidence_sources": rule["evidence"],
        }
        for rule in active_rules
    ]


def build_rbl_contradiction_paragraph(active_rules: list[dict]) -> str:
    if not active_rules:
        return "No institutionally meaningful contradiction is currently detected. GeoScen remains in monitored synthesis mode."

    sentences = [rule["rbl_sentence"] for rule in active_rules]
    return " ".join(sentences)


def temperature_adjustment(active_rules: list[dict]) -> dict:
    if not active_rules:
        return {
            "adjustment": 0.0,
            "reason": "No active contradiction pressure.",
        }

    max_score = max(rule["score"] for rule in active_rules)
    adjustment = round(min(0.25, max_score * 0.20), 4)

    return {
        "adjustment": adjustment,
        "reason": "Contradiction pressure increases monitoring temperature.",
    }


def main() -> None:
    frontend = read_json(SOURCE_CONTRACT["frontend"])
    rbl = read_json(SOURCE_CONTRACT["rbl"])
    cb = read_latest_parquet(SOURCE_CONTRACT["cb"])
    c_flow = read_json(SOURCE_CONTRACT["c_flow"])
    breadth = read_latest_parquet(SOURCE_CONTRACT["breadth"])

    active_rules = build_contradiction_rules(
        rbl=rbl,
        cb=cb,
        c_flow=c_flow,
        breadth=breadth,
    )

    matrix = build_divergence_matrix(active_rules)
    temp_adj = temperature_adjustment(active_rules)

    contradiction_score = round(
        max([r["score"] for r in active_rules], default=0.0),
        4,
    )

    payload = {
        "component": "GeoScen Contradiction Engine",
        "version": "v1",
        "built_at_utc": datetime.now(timezone.utc).isoformat(),
        "contradiction_score": contradiction_score,
        "contradiction_severity": severity(contradiction_score),
        "active_contradiction_count": len(active_rules),
        "active_rules": active_rules,
        "divergence_matrix": matrix,
        "temperature_adjustment": temp_adj,
        "rbl_contradiction_paragraph": build_rbl_contradiction_paragraph(active_rules),
        "source_evidence": {
            "frontend_available": frontend.get("available"),
            "rbl_available": rbl.get("available"),
            "cb_available": cb.get("available"),
            "c_flow_available": c_flow.get("available"),
            "breadth_available": breadth.get("available"),
        },
        "governance": {
            "rules_based": True,
            "ai_last": True,
            "explainable": True,
            "source_provenance_required": True,
            "frontend_highlight_ready": True,
        },
    }

    out_json = OUT_DIR / "geoscen_contradiction_engine_v1.json"
    out_txt = OUT_DIR / "geoscen_contradiction_engine_v1.txt"

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    with open(out_txt, "w", encoding="utf-8-sig") as f:
        f.write("GEOSCEN CONTRADICTION ENGINE V1\n")
        f.write("=" * 60 + "\n\n")
        f.write(f"contradiction_score: {payload['contradiction_score']}\n")
        f.write(f"contradiction_severity: {payload['contradiction_severity']}\n")
        f.write(f"active_contradiction_count: {payload['active_contradiction_count']}\n\n")

        f.write("DIVERGENCE MATRIX\n")
        f.write("-" * 60 + "\n")
        if matrix:
            for row in matrix:
                f.write(
                    f"- {row['severity'].upper()} | "
                    f"{row['axis']} | "
                    f"score={row['score']} | "
                    f"evidence={', '.join(row['evidence_sources'])}\n"
                )
        else:
            f.write("- None detected\n")

        f.write("\nRBL CONTRADICTION PARAGRAPH\n")
        f.write("-" * 60 + "\n")
        f.write(payload["rbl_contradiction_paragraph"] + "\n")

        f.write("\nTEMPERATURE ADJUSTMENT\n")
        f.write("-" * 60 + "\n")
        f.write(f"adjustment: {temp_adj['adjustment']}\n")
        f.write(f"reason: {temp_adj['reason']}\n")

    print("OK | GeoScen Contradiction Engine v1 built")
    print(f"contradiction_score    : {payload['contradiction_score']}")
    print(f"contradiction_severity : {payload['contradiction_severity']}")
    print(f"active_count           : {payload['active_contradiction_count']}")
    print("\nArtifacts written:")
    print(out_json)
    print(out_txt)


if __name__ == "__main__":
    main()

    
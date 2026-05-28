from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd


REPO_ROOT = Path.cwd()

OUT_DIR = REPO_ROOT / "data" / "serving" / "geoscen" / "frontend"
OUT_DIR.mkdir(parents=True, exist_ok=True)

RBL_JSON = REPO_ROOT / "data" / "serving" / "geoscen" / "geoscen_rbl_synthesis_v1.json"

SOURCE_CONTRACT = {
    "geoscen_rbl": REPO_ROOT / "data" / "serving" / "geoscen" / "geoscen_rbl_synthesis_v1.json",
    "c_flow": REPO_ROOT / "data" / "serving" / "c_flow" / "c_flow_latest_v5.json",
    "rates": REPO_ROOT / "data" / "serving" / "rates" / "rates_zt_latest.json",
    "fx": REPO_ROOT / "data" / "serving" / "fx" / "fx_latest.json",
    "wti": REPO_ROOT / "data" / "serving" / "wti" / "wti_panel.json",
    "equities": REPO_ROOT / "data" / "serving" / "equities" / "breadth_factor_serving_v1.parquet",
}


def read_json(path: Path) -> dict:
    if not path.exists():
        return {"available": False, "path": str(path)}
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    if isinstance(obj, dict):
        obj["available"] = True
    return obj

def normalize_payload(obj):
    if isinstance(obj, dict):
        return obj

    if isinstance(obj, list):
        if not obj:
            return {"available": False, "reason": "empty_list"}
        if isinstance(obj[-1], dict):
            out = obj[-1]
            out["available"] = True
            return out
        return {"available": True, "value": obj[-1]}

    return {"available": False, "value": str(obj)}

def read_latest_parquet(path: Path) -> dict:
    if not path.exists():
        return {"available": False, "path": str(path)}

    df = pd.read_parquet(path)
    if df.empty:
        return {"available": False, "path": str(path), "reason": "empty"}

    row = df.tail(1).iloc[0].to_dict()
    row["available"] = True
    return row


def classify_temperature(score: float | None) -> str:
    if score is None:
        return "UNKNOWN"
    if score >= 0.70:
        return "HOT"
    if score >= 0.45:
        return "WARM"
    if score >= 0.20:
        return "COOL"
    return "COLD"


def build_graph_routes(payload: dict) -> list[dict]:
    temp = payload.get("temperature", "UNKNOWN")

    routes = [
        {
            "slot": "top_left",
            "title": "Zeitgeist (Zₜ)",
            "component": "temperature_card",
            "priority": 1,
            "reason": "Core GeoScen regime state.",
        },
        {
            "slot": "top_right",
            "title": "Graph Area",
            "component": "adaptive_chart",
            "priority": 2,
            "preferred_series": ["temperature_score", "c_flow", "rates", "fx", "breadth"],
            "reason": "Visual proof for current macro state.",
        },
        {
            "slot": "bottom_left",
            "title": "RBL",
            "component": "rbl_narrative_panel",
            "priority": 3,
            "reason": "Institutional interpretation layer.",
        },
        {
            "slot": "bottom_right",
            "title": "Final Metric",
            "component": "final_metric_stack",
            "priority": 4,
            "reason": "Compressed decision-support output.",
        },
    ]

    if temp in {"HOT", "WARM"}:
        routes.insert(
            1,
            {
                "slot": "top_right",
                "title": "Contradiction Focus",
                "component": "contradiction_chart",
                "priority": 1.5,
                "reason": "Elevated regime requires divergence evidence first.",
            },
        )

    return routes


def build_contradictions(rbl: dict) -> list[dict]:
    text = str(rbl.get("rbl", "")).lower()

    checks = [
        {
            "name": "hawkish_policy_vs_weak_growth",
            "trigger": "hawkish" in text and ("weakening" in text or "weak breadth" in text),
            "severity": "medium",
            "description": "Policy tone appears restrictive while growth or breadth weakens.",
        },
        {
            "name": "cool_temperature_but_divergence_flagged",
            "trigger": rbl.get("temperature") == "COOL" and "divergence flagged" in text,
            "severity": "medium",
            "description": "Headline temperature is calm, but policy divergence is active.",
        },
        {
            "name": "risk_on_breadth_vs_macro_softening",
            "trigger": "risk-on" in text and ("weakening manufacturing" in text or "falling pmi" in text),
            "severity": "low",
            "description": "Equity participation remains firm while macro-industrial tone softens.",
        },
    ]

    return [c for c in checks if c["trigger"]]


def first_available(payload: dict, keys: list[str], default: str = "Unavailable"):
    for key in keys:
        value = payload.get(key)
        if value is not None and value != "":
            return value
    return default

def contains_any(text: str, terms: list[str]) -> bool:
    return any(term in text for term in terms)


def build_contradiction_highlights(rbl: dict, evidence_cards: list[dict]) -> dict:
    text = str(rbl.get("rbl", "")).lower()
    temp = str(rbl.get("temperature", "UNKNOWN")).upper()

    highlights = []

    if contains_any(text, ["hawkish"]) and contains_any(text, ["weakening", "weak breadth", "falling pmi", "weak manufacturing"]):
        highlights.append({
            "id": "policy_growth_divergence",
            "severity": "medium",
            "title": "Policy / Growth Divergence",
            "summary": "Policy tone is restrictive while growth or breadth evidence is softening.",
            "ui_action": "highlight_cb_and_growth_cards",
            "recommended_panel": "Contradiction Focus",
        })

    if temp == "COOL" and contains_any(text, ["divergence flagged", "policy divergence"]):
        highlights.append({
            "id": "cool_state_hidden_divergence",
            "severity": "medium",
            "title": "Cool Temperature / Hidden Divergence",
            "summary": "Headline regime is cool, but divergence language is active.",
            "ui_action": "badge_temperature_card",
            "recommended_panel": "Zeitgeist (Zₜ)",
        })

    if contains_any(text, ["risk-on", "healthy breadth"]) and contains_any(text, ["weakening manufacturing", "weakening", "falling pmi"]):
        highlights.append({
            "id": "risk_on_macro_softening",
            "severity": "low",
            "title": "Risk-On / Macro Softening",
            "summary": "Equity participation remains healthy while industrial macro tone weakens.",
            "ui_action": "highlight_breadth_and_pmi_cards",
            "recommended_panel": "Graph Area",
        })

    return {
        "enabled": True,
        "highlight_count": len(highlights),
        "highest_severity": (
            "high" if any(h["severity"] == "high" for h in highlights)
            else "medium" if any(h["severity"] == "medium" for h in highlights)
            else "low" if highlights
            else "none"
        ),
        "highlights": highlights,
        "visual_rules": {
            "show_contradiction_badges": True,
            "promote_medium_or_high_to_top_right": True,
            "never_hide_normal_evidence": True,
            "display_empty_state_when_none": True,
        },
    }

def build_temperature_aware_ui(rbl: dict, contradiction_highlighting: dict) -> dict:
    temp = str(rbl.get("temperature", "UNKNOWN")).upper()
    score = rbl.get("temperature_score")

    if score is not None:
        score = float(score)

    if temp == "HOT":
        mode = "risk_alert"
        emphasis = "maximum"
        refresh_priority = "high"
        primary_panel = "Contradiction Focus"
    elif temp == "WARM":
        mode = "active_monitoring"
        emphasis = "elevated"
        refresh_priority = "medium_high"
        primary_panel = "Graph Area"
    elif temp == "COOL":
        mode = "balanced_monitoring"
        emphasis = "normal"
        refresh_priority = "medium"
        primary_panel = "Zeitgeist (Zₜ)"
    elif temp == "COLD":
        mode = "low_activity_monitoring"
        emphasis = "low"
        refresh_priority = "low"
        primary_panel = "RBL"
    else:
        mode = "unknown_state"
        emphasis = "fallback"
        refresh_priority = "medium"
        primary_panel = "Zeitgeist (Zₜ)"

    if contradiction_highlighting.get("highest_severity") in {"medium", "high"}:
        primary_panel = "Contradiction Focus"
        emphasis = "elevated"

    return {
        "enabled": True,
        "temperature": temp,
        "temperature_score": score,
        "ui_mode": mode,
        "visual_emphasis": emphasis,
        "primary_panel": primary_panel,
        "refresh_priority": refresh_priority,
        "layout_rules": {
            "show_temperature_badge": True,
            "show_score_gradient": True,
            "promote_contradictions_when_active": True,
            "keep_rbl_visible": True,
            "never_hide_source_evidence": True,
        },
        "panel_behavior": {
            "top_left": {
                "component": "temperature_card",
                "state": temp,
                "badge": f"{temp} regime",
                "locked": True,
            },
            "top_right": {
                "component": "adaptive_chart",
                "state": primary_panel,
                "promote_if": "medium_or_high_contradiction",
            },
            "bottom_left": {
                "component": "rbl_narrative_panel",
                "state": "always_visible",
            },
            "bottom_right": {
                "component": "final_metric_stack",
                "state": "temperature_weighted",
            },
        },
    }

def build_evidence_visualization_payload(rbl: dict, c_flow: dict, rates: dict, fx: dict, wti: dict, equities: dict) -> dict:
    return {
        "primary_visual": {
            "title": "GeoScen Evidence Stack",
            "chart_type": "multi_signal_status_panel",
            "purpose": "Show why the current GeoScen temperature & RBL are justified.",
        },
        "evidence_cards": [
            {
                "label": "Temperature",
                "value": first_available(rbl, ["temperature"]),
                "score": first_available(rbl, ["temperature_score"]),
                "source": "geoscen_rbl_synthesis_v1.json",
                "ui_slot": "top_left",
            },
            {
                "label": "Capital Flow",
                "value": first_available(c_flow, ["regime", "c_flow_regime", "label", "state"]),
                "score": first_available(c_flow, ["fund_flow_pressure", "c_flow_score", "score", "temperature_score"]),
                "source": "c_flow_latest_v5.json",
                "ui_slot": "top_right",
            },
            {
                "label": "Rates",
                "value": first_available(rates, ["regime", "rates_regime", "label", "state"]),
                "score": first_available(rates, ["zt_score", "temperature_score", "score", "value"]),
                "source": "rates_zt_latest.json",
                "ui_slot": "top_right",
            },
            {
                "label": "FX",
                "value": first_available(fx, ["regime", "fx_regime", "label", "state", "pair"]),
                "score": first_available(fx, ["zt_score", "stress_score", "score", "value"]),
                "source": "fx_latest.json",
                "ui_slot": "top_right",
            },
            {
                "label": "WTI",
                "value": first_available(wti, ["regime", "wti_regime", "label", "state"]),
                "score": first_available(wti, ["zt_score", "pressure_score", "score", "value"]),
                "source": "wti_panel.json",
                "ui_slot": "top_right",
            },
            {
                "label": "Breadth",
                "value": first_available(equities, ["regime", "breadth_regime", "label", "state"]),
                "score": first_available(equities, ["breadth_factor_score", "score", "value"]),
                "source": "breadth_factor_serving_v1.parquet",
                "ui_slot": "top_right",
            },
        ],
        "visual_rules": {
            "show_source_badges": True,
            "show_missing_sources": True,
            "prioritize_contradictions": True,
            "fallback_when_missing": "Show unavailable source card rather than hiding evidence.",
        },
    }

def main() -> None:
    rbl = read_json(SOURCE_CONTRACT["geoscen_rbl"])
    c_flow = normalize_payload(read_json(SOURCE_CONTRACT["c_flow"]))
    rates = normalize_payload(read_json(SOURCE_CONTRACT["rates"]))
    fx = normalize_payload(read_json(SOURCE_CONTRACT["fx"]))
    wti = normalize_payload(read_json(SOURCE_CONTRACT["wti"]))
    equities = read_latest_parquet(SOURCE_CONTRACT["equities"])

    temp_score = rbl.get("temperature_score")
    if temp_score is not None:
        temp_score = float(temp_score)

    frontend_payload = {
        "component": "GeoScen Frontend Intelligence Layer",
        "version": "v1",
        "built_at_utc": datetime.now(timezone.utc).isoformat(),
        "temperature": rbl.get("temperature") or classify_temperature(temp_score),
        "temperature_score": temp_score,
        "rbl": rbl.get("rbl"),
        "source_route": rbl.get("source_route", []),

        "graph_routes": build_graph_routes(rbl),

        "contradictions": build_contradictions(rbl),

        "evidence": {
            "geoscen_rbl": rbl,
            "c_flow": c_flow,
            "rates": rates,
            "fx": fx,
            "wti": wti,
            "equities": equities,
        },

        "evidence_visualization": build_evidence_visualization_payload(
            rbl=rbl,
            c_flow=c_flow,
            rates=rates,
            fx=fx,
            wti=wti,
            equities=equities,
        ),

        "contradiction_highlighting": build_contradiction_highlights(
            rbl=rbl,
            evidence_cards=build_evidence_visualization_payload(
                rbl=rbl,
                c_flow=c_flow,
                rates=rates,
                fx=fx,
                wti=wti,
                equities=equities,
            )["evidence_cards"],
        ),

        "temperature_aware_ui": build_temperature_aware_ui(
            rbl=rbl,
            contradiction_highlighting=build_contradiction_highlights(
                rbl=rbl,
                evidence_cards=build_evidence_visualization_payload(
                    rbl=rbl,
                    c_flow=c_flow,
                    rates=rates,
                    fx=fx,
                    wti=wti,
                    equities=equities,
                )["evidence_cards"],
            ),
        ),

        "ui_contract": {
            "top_left": "Zeitgeist card",
            "top_right": "Adaptive evidence graph",
            "bottom_left": "RBL interpretation",
            "bottom_right": "Final metric stack",
        },

        "governance": {
            "yahoo_finance_allowed": False,
            "ai_last": True,
            "cross_asset_linkage_required": True,
            "frontend_is_serving_layer_only": True,
        },
    }

    out_json = OUT_DIR / "geoscen_frontend_intelligence_layer_v1.json"
    out_txt = OUT_DIR / "geoscen_frontend_intelligence_layer_v1.txt"

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(frontend_payload, f, indent=2, default=str)

    with open(out_txt, "w", encoding="utf-8-sig") as f:

        f.write("GEOSCEN FRONTEND INTELLIGENCE LAYER V1\n")
        f.write("=" * 60 + "\n\n")

        f.write(f"temperature: {frontend_payload['temperature']}\n")
        f.write(f"temperature_score: {frontend_payload['temperature_score']}\n\n")

        f.write("GRAPH ROUTES\n")
        f.write("-" * 60 + "\n")

        for route in frontend_payload["graph_routes"]:
            f.write(
                f"- {route['slot']} | "
                f"{route['title']} | "
                f"{route['component']}\n"
            )

        f.write("\nCONTRADICTIONS\n")
        f.write("-" * 60 + "\n")

        if frontend_payload["contradictions"]:
            for c in frontend_payload["contradictions"]:
                f.write(
                    f"- {c['severity'].upper()} | "
                    f"{c['name']} | "
                    f"{c['description']}\n"
                )
        else:
            f.write("- None detected\n")

        f.write("\nEVIDENCE VISUALIZATION\n")
        f.write("-" * 60 + "\n")

        for card in frontend_payload["evidence_visualization"]["evidence_cards"]:
            f.write(
                f"- {card['label']} | "
                f"value={card['value']} | "
                f"score={card['score']} | "
                f"source={card['source']}\n"
            )

        f.write("\nCONTRADICTION HIGHLIGHTING\n")
        f.write("-" * 60 + "\n")

        ch = frontend_payload["contradiction_highlighting"]

        f.write(f"enabled: {ch['enabled']}\n")
        f.write(f"highlight_count: {ch['highlight_count']}\n")
        f.write(f"highest_severity: {ch['highest_severity']}\n")

        if ch["highlights"]:
            for h in ch["highlights"]:
                f.write(
                    f"- {h['severity'].upper()} | "
                    f"{h['title']} | "
                    f"{h['summary']} | "
                    f"ui_action={h['ui_action']}\n"
                )
        else:
            f.write("- None detected\n")

        f.write("\nTEMPERATURE-AWARE UI\n")
        f.write("-" * 60 + "\n")

        tui = frontend_payload["temperature_aware_ui"]

        f.write(f"enabled: {tui['enabled']}\n")
        f.write(f"temperature: {tui['temperature']}\n")
        f.write(f"temperature_score: {tui['temperature_score']}\n")
        f.write(f"ui_mode: {tui['ui_mode']}\n")
        f.write(f"visual_emphasis: {tui['visual_emphasis']}\n")
        f.write(f"primary_panel: {tui['primary_panel']}\n")
        f.write(f"refresh_priority: {tui['refresh_priority']}\n")

    print("\nArtifacts written:")
    print(out_json)
    print(out_txt)


if __name__ == "__main__":
    main()

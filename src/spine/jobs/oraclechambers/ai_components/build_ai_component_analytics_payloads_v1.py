from pathlib import Path
from datetime import datetime, timezone
import json


REPO_ROOT = Path(__file__).resolve().parents[5]
ROOT = REPO_ROOT / "_offline_site" / "oc_ai_components"

PLANES = {
    "fx": {
        "payload": "fx_ai_components_v1.json",
        "analytics": {
            "z_t_composite": 0.72,
            "signal_strength": "High",
            "stress_score": 0.81,
            "dispersion_score": 0.63,
            "contradiction_score": 0.42,
            "momentum_score": 0.58,
            "confidence_score": 0.895,
            "conviction_score": 0.6694,
            "model_mode": "Deterministic AI cognition component",
            "ml_readiness": "Feature-ready; not trained ML",
        },
        "features": [
            {"feature": "USD funding pressure", "weight": 0.24, "direction": "stress_positive"},
            {"feature": "Carry unwind risk", "weight": 0.19, "direction": "stress_positive"},
            {"feature": "Central-bank divergence", "weight": 0.22, "direction": "fragmentation_positive"},
            {"feature": "Cross-asset liquidity confirmation", "weight": 0.17, "direction": "confirmation_negative"},
        ],
        "analogs": [
            {"regime": "1998 FX Contagion", "similarity": 0.65, "why": "FX fragmentation & liquidity stress"},
            {"regime": "2008 Liquidity Stress", "similarity": 0.58, "why": "Funding pressure & risk compression"},
            {"regime": "2022 Tightening Cycle", "similarity": 0.54, "why": "Policy divergence & USD pressure"},
        ],
        "decision": {
            "risk_posture": "Defensive monitoring",
            "confirmation_status": "Fragmented",
            "action_bias": "Monitor / avoid broad risk confirmation",
            "fragility_level": "Moderate-High",
        },
    },
    "rates": {
        "payload": "rates_ai_components_v1.json",
        "analytics": {
            "z_t_composite": 0.69,
            "signal_strength": "Moderate-High",
            "stress_score": 0.76,
            "dispersion_score": 0.61,
            "contradiction_score": 0.48,
            "momentum_score": 0.55,
            "confidence_score": 0.895,
            "conviction_score": 0.6694,
            "model_mode": "Deterministic AI cognition component",
            "ml_readiness": "Feature-ready; not trained ML",
        },
        "features": [
            {"feature": "Duration pressure", "weight": 0.25, "direction": "stress_positive"},
            {"feature": "Curve instability", "weight": 0.21, "direction": "fragility_positive"},
            {"feature": "Policy restriction", "weight": 0.20, "direction": "liquidity_negative"},
            {"feature": "Term premium pressure", "weight": 0.16, "direction": "risk_positive"},
        ],
        "analogs": [
            {"regime": "2022 Tightening Cycle", "similarity": 0.67, "why": "Restrictive policy & duration repricing"},
            {"regime": "1994 Bond Shock", "similarity": 0.59, "why": "Rates volatility & curve stress"},
            {"regime": "2008 Liquidity Stress", "similarity": 0.52, "why": "Liquidity pressure & flight-to-quality risk"},
        ],
        "decision": {
            "risk_posture": "Defensive duration awareness",
            "confirmation_status": "Incomplete",
            "action_bias": "Do not overtrust equity strength while rates pressure persists",
            "fragility_level": "Moderate",
        },
    },
    "c_flow": {
        "payload": "cflow_ai_components_v1.json",
        "analytics": {
            "z_t_composite": 0.64,
            "signal_strength": "Moderate",
            "stress_score": 0.66,
            "dispersion_score": 0.71,
            "contradiction_score": 0.39,
            "momentum_score": 0.49,
            "confidence_score": 0.895,
            "conviction_score": 0.6694,
            "model_mode": "Deterministic AI cognition component",
            "ml_readiness": "Feature-ready; not trained ML",
        },
        "features": [
            {"feature": "Energy volatility", "weight": 0.23, "direction": "inflation_positive"},
            {"feature": "Industrial metals weakness", "weight": 0.18, "direction": "growth_negative"},
            {"feature": "Supply fragmentation", "weight": 0.21, "direction": "stress_positive"},
            {"feature": "Commodity inflation persistence", "weight": 0.17, "direction": "inflation_positive"},
        ],
        "analogs": [
            {"regime": "1970s Inflation Shock", "similarity": 0.50, "why": "Commodity pressure & supply stress"},
            {"regime": "2022 Energy Shock", "similarity": 0.62, "why": "Energy sensitivity & inflation persistence"},
            {"regime": "2008 Commodity Reversal", "similarity": 0.47, "why": "Growth pressure & flow reversal risk"},
        ],
        "decision": {
            "risk_posture": "Inflation-sensitive monitoring",
            "confirmation_status": "Mixed",
            "action_bias": "Avoid assuming full disinflation confirmation",
            "fragility_level": "Moderate",
        },
    },
    "equities_sector": {
        "payload": "equities_sector_ai_components_v1.json",
        "analytics": {
            "z_t_composite": 0.67,
            "signal_strength": "Moderate",
            "stress_score": 0.52,
            "dispersion_score": 0.74,
            "contradiction_score": 0.44,
            "momentum_score": 0.62,
            "confidence_score": 0.895,
            "conviction_score": 0.6694,
            "model_mode": "Deterministic AI cognition component",
            "ml_readiness": "Feature-ready; not trained ML",
        },
        "features": [
            {"feature": "Sector breadth participation", "weight": 0.23, "direction": "confirmation_negative"},
            {"feature": "Defensive/cyclical rotation", "weight": 0.20, "direction": "dispersion_positive"},
            {"feature": "Leadership concentration", "weight": 0.22, "direction": "fragility_positive"},
            {"feature": "Cross-sector confirmation", "weight": 0.18, "direction": "confirmation_negative"},
        ],
        "analogs": [
            {"regime": "1999 Narrow Leadership", "similarity": 0.61, "why": "Leadership concentration"},
            {"regime": "2020 Growth Leadership", "similarity": 0.55, "why": "Sector concentration & dispersion"},
            {"regime": "2022 Rotation Shock", "similarity": 0.53, "why": "Rates-sensitive sector rotation"},
        ],
        "decision": {
            "risk_posture": "Selective participation awareness",
            "confirmation_status": "Fragmented",
            "action_bias": "Do not treat narrow leadership as broad confirmation",
            "fragility_level": "Moderate",
        },
    },
    "equities_industry": {
        "payload": "equities_industry_ai_components_v1.json",
        "analytics": {
            "z_t_composite": 0.65,
            "signal_strength": "Moderate",
            "stress_score": 0.50,
            "dispersion_score": 0.77,
            "contradiction_score": 0.41,
            "momentum_score": 0.60,
            "confidence_score": 0.895,
            "conviction_score": 0.6694,
            "model_mode": "Deterministic AI cognition component",
            "ml_readiness": "Feature-ready; not trained ML",
        },
        "features": [
            {"feature": "Industry breadth participation", "weight": 0.24, "direction": "confirmation_negative"},
            {"feature": "Leadership concentration", "weight": 0.23, "direction": "fragility_positive"},
            {"feature": "Cyclical industry confirmation", "weight": 0.20, "direction": "confirmation_negative"},
            {"feature": "Industry dispersion pressure", "weight": 0.19, "direction": "dispersion_positive"},
        ],
        "analogs": [
            {"regime": "1999 Industry Concentration", "similarity": 0.60, "why": "Narrow leadership beneath index strength"},
            {"regime": "2021 Reopening Rotation", "similarity": 0.49, "why": "Cyclical confirmation instability"},
            {"regime": "2022 Dispersion Shock", "similarity": 0.56, "why": "Industry dispersion & rates sensitivity"},
        ],
        "decision": {
            "risk_posture": "Industry-level confirmation discipline",
            "confirmation_status": "Incomplete",
            "action_bias": "Wait for industry breadth before confirming broad market strength",
            "fragility_level": "Moderate",
        },
    },
}


def enrich_plane(site: str, config: dict) -> dict:
    path = ROOT / site / "payloads" / config["payload"]
    payload = json.loads(path.read_text(encoding="utf-8"))

    payload["analytics"] = config["analytics"]
    payload["feature_vector"] = config["features"]
    payload["historical_analogs"] = config["analogs"]
    payload["executive_decision_layer"] = config["decision"]
    payload["client_visible_intelligence"] = True
    payload["analytics_version"] = "institutional_analytics_v1"

    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    return {
        "site": site,
        "path": str(path),
        "enriched": True,
    }


def main() -> None:
    results = [enrich_plane(site, config) for site, config in PLANES.items()]

    print(json.dumps({
        "artifact": "build_ai_component_analytics_payloads_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "analytics_ready": True,
        "client_visible_intelligence": True,
        "results": results,
    }, indent=2))


if __name__ == "__main__":
    main()

    
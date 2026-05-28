from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd


REPO_ROOT = Path.cwd()

OUT_DIR = REPO_ROOT / "data" / "serving" / "geoscen" / "compartments" / "rates"
OUT_DIR.mkdir(parents=True, exist_ok=True)

SOURCE_CONTRACT = {
    "rates_zt_latest": REPO_ROOT / "data" / "serving" / "rates" / "rates_zt_latest.json",
    "rates_zt_panel": REPO_ROOT / "data" / "serving" / "rates" / "rates_zt_panel.json",
    "rates_serving": REPO_ROOT / "data" / "serving" / "rates" / "rates_serving_v2.parquet",
    "rates_curve": REPO_ROOT / "data" / "serving" / "rates" / "rates_curve_data.json",
    "rates_policy": REPO_ROOT / "data" / "serving" / "rates" / "rates_policy_pressure_data.json",
    "rates_sigma": REPO_ROOT / "data" / "serving" / "rates" / "rates_sigma_rank.json",
    "china_policy": REPO_ROOT / "data" / "serving" / "rates" / "china" / "china_policy.json",
    "china_y10_proxy": REPO_ROOT / "data" / "serving" / "rates" / "china" / "china_y10_proxy.json",
    "frontend": REPO_ROOT / "data" / "serving" / "geoscen" / "frontend" / "geoscen_frontend_intelligence_layer_v1.json",
    "contradiction": REPO_ROOT / "data" / "serving" / "geoscen" / "contradiction" / "geoscen_contradiction_engine_v1.json",
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

    row = df.tail(1).iloc[0].to_dict()
    row["available"] = True
    return row


def build_readiness(rows: list[dict]) -> dict:
    total = len(rows)
    ready = sum(1 for row in rows if row["available"])
    ratio = round(ready / max(1, total), 4)

    if ratio >= 0.95:
        state = "ready"
    elif ratio >= 0.75:
        state = "degraded_ready"
    else:
        state = "not_ready"

    return {
        "required_count": total,
        "ready_count": ready,
        "readiness_ratio": ratio,
        "readiness_state": state,
    }


def first_available(payload: dict, keys: list[str], default=None):
    for key in keys:
        value = payload.get(key)
        if value is not None and value != "":
            return value
    return default


def main() -> None:
    rates_zt_latest = read_json(SOURCE_CONTRACT["rates_zt_latest"])
    rates_zt_panel = read_json(SOURCE_CONTRACT["rates_zt_panel"])
    rates_serving = read_latest_parquet(SOURCE_CONTRACT["rates_serving"])
    rates_curve = read_json(SOURCE_CONTRACT["rates_curve"])
    rates_policy = read_json(SOURCE_CONTRACT["rates_policy"])
    rates_sigma = read_json(SOURCE_CONTRACT["rates_sigma"])
    china_policy = read_json(SOURCE_CONTRACT["china_policy"])
    china_y10_proxy = read_json(SOURCE_CONTRACT["china_y10_proxy"])
    frontend = read_json(SOURCE_CONTRACT["frontend"])
    contradiction = read_json(SOURCE_CONTRACT["contradiction"])

    payloads = {
        "rates_zt_latest": rates_zt_latest,
        "rates_zt_panel": rates_zt_panel,
        "rates_serving": rates_serving,
        "rates_curve": rates_curve,
        "rates_policy": rates_policy,
        "rates_sigma": rates_sigma,
        "china_policy": china_policy,
        "china_y10_proxy": china_y10_proxy,
        "frontend": frontend,
        "contradiction": contradiction,
    }

    source_rows = [
        {
            "component": name,
            "available": obj.get("available", False),
            "path": str(SOURCE_CONTRACT[name]),
        }
        for name, obj in payloads.items()
    ]

    readiness = build_readiness(source_rows)

    rates_score = first_available(
        rates_zt_latest,
        ["zt_score", "rates_score", "pressure_score", "score", "value"],
        default=0.35,
    )

    contradiction_score = first_available(
        contradiction,
        ["contradiction_score"],
        default=0.0,
    )

    if float(rates_score) >= 0.70:
        zt_label = "High Rates / Policy Pressure"
    elif float(rates_score) >= 0.45:
        zt_label = "Balanced Rates Pressure"
    else:
        zt_label = "Low-Conviction / Monitoring Regime"

    final_metric = round(float(rates_score) * 100, 2)

    compartment = {
        "component": "GeoScen RATES Compartment",
        "version": "v1",
        "built_at_utc": datetime.now(timezone.utc).isoformat(),

        "compartment": "rates",
        "deployment_role": "rates_curve_policy_sovereign_pressure_compartment",

        "zt": {
            "label": zt_label,
            "rates_pressure_score": rates_score,
            "final_metric_0_100": final_metric,
        },

        "rates_context": {
            "rates_zt_latest_available": rates_zt_latest.get("available"),
            "rates_zt_panel_available": rates_zt_panel.get("available"),
            "rates_serving_available": rates_serving.get("available"),
            "rates_curve_available": rates_curve.get("available"),
            "rates_policy_available": rates_policy.get("available"),
            "rates_sigma_available": rates_sigma.get("available"),
            "china_policy_available": china_policy.get("available"),
            "china_y10_proxy_available": china_y10_proxy.get("available"),
            "frontend_available": frontend.get("available"),
            "contradiction_score": contradiction_score,
        },

        "graph_area": [
            "rates_zt_latest",
            "rates_zt_panel",
            "rates_serving_v2",
            "rates_curve_data",
            "rates_policy_pressure_data",
            "rates_sigma_rank",
            "china_policy",
            "china_y10_proxy",
            "contradiction_engine",
            "geoscen_frontend_overlay",
        ],

        "rbl": {
            "text": (
                f"RATES reads as {zt_label}. "
                f"Rates pressure score is currently {rates_score}. "
                f"This compartment captures curve pressure, policy-pressure routing, sovereign stress, "
                f"and China rates integration through the China policy and 10Y proxy channels."
            ),
        },

        "readiness": readiness,
        "source_rows": source_rows,

        "frontend_contract": {
            "panel_title": "RATES",
            "primary_fields": [
                "Zₜ",
                "Rates Pressure",
                "Curve Pressure",
                "Policy Pressure",
                "Sovereign Stress",
                "China Rates Overlay",
                "Contradiction Overlay",
                "Final Metric",
            ],
            "deployable": readiness["readiness_state"] in {"ready", "degraded_ready"},
        },

        "governance": {
            "compartmentalized": True,
            "curve_pressure_required": True,
            "policy_pressure_required": True,
            "china_overlay_included": True,
            "source_provenance_required": True,
            "deploy_all_together_later": True,
            "ai_last": True,
        },
    }

    out_json = OUT_DIR / "geoscen_rates_compartment_v1.json"
    out_txt = OUT_DIR / "geoscen_rates_compartment_v1.txt"

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(compartment, f, indent=2, default=str)

    with open(out_txt, "w", encoding="utf-8-sig") as f:
        f.write("GEOSCEN RATES COMPARTMENT V1\n")
        f.write("=" * 60 + "\n\n")

        f.write(f"zt_label: {compartment['zt']['label']}\n")
        f.write(f"rates_pressure_score: {compartment['zt']['rates_pressure_score']}\n")
        f.write(f"final_metric_0_100: {compartment['zt']['final_metric_0_100']}\n\n")

        f.write("RATES CONTEXT\n")
        f.write("-" * 60 + "\n")
        for k, v in compartment["rates_context"].items():
            f.write(f"{k}: {v}\n")

        f.write("\nGRAPH AREA\n")
        f.write("-" * 60 + "\n")
        for item in compartment["graph_area"]:
            f.write(f"- {item}\n")

        f.write("\nREADINESS\n")
        f.write("-" * 60 + "\n")
        for k, v in readiness.items():
            f.write(f"{k}: {v}\n")

        f.write("\nRBL\n")
        f.write("-" * 60 + "\n")
        f.write(compartment["rbl"]["text"] + "\n")

    print("OK | GeoScen RATES Compartment v1 built")
    print(f"zt_label         : {compartment['zt']['label']}")
    print(f"final_metric     : {compartment['zt']['final_metric_0_100']}")
    print(f"readiness_state  : {readiness['readiness_state']}")
    print(f"readiness_ratio  : {readiness['readiness_ratio']}")

    print("\nArtifacts written:")
    print(out_json)
    print(out_txt)


if __name__ == "__main__":
    main()
    
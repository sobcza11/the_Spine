from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd


REPO_ROOT = Path.cwd()

OUT_DIR = REPO_ROOT / "data" / "serving" / "geoscen" / "compartments" / "fx"
OUT_DIR.mkdir(parents=True, exist_ok=True)

SOURCE_CONTRACT = {
    "fx_latest": REPO_ROOT / "data" / "serving" / "fx" / "fx_latest.json",
    "fx_serving": REPO_ROOT / "data" / "serving" / "fx" / "fx_serving_v2.parquet",
    "fx_zt": REPO_ROOT / "data" / "serving" / "fx" / "fx_zt_v1.parquet",
    "fx_price_data": REPO_ROOT / "data" / "serving" / "fx" / "fx_price_data.json",
    "fx_sigma_data": REPO_ROOT / "data" / "serving" / "fx" / "fx_sigma_data.json",
    "fx_spreads_data": REPO_ROOT / "data" / "serving" / "fx" / "fx_spreads_data.json",
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
    fx_latest = read_json(SOURCE_CONTRACT["fx_latest"])
    fx_serving = read_latest_parquet(SOURCE_CONTRACT["fx_serving"])
    fx_zt = read_latest_parquet(SOURCE_CONTRACT["fx_zt"])
    fx_price_data = read_json(SOURCE_CONTRACT["fx_price_data"])
    fx_sigma_data = read_json(SOURCE_CONTRACT["fx_sigma_data"])
    fx_spreads_data = read_json(SOURCE_CONTRACT["fx_spreads_data"])
    frontend = read_json(SOURCE_CONTRACT["frontend"])
    contradiction = read_json(SOURCE_CONTRACT["contradiction"])

    payloads = {
        "fx_latest": fx_latest,
        "fx_serving": fx_serving,
        "fx_zt": fx_zt,
        "fx_price_data": fx_price_data,
        "fx_sigma_data": fx_sigma_data,
        "fx_spreads_data": fx_spreads_data,
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

    fx_score = first_available(
        fx_latest,
        ["fx_pressure_score", "score", "signal_strength"],
        default=0.35,
    )

    contradiction_score = first_available(
        contradiction,
        ["contradiction_score"],
        default=0.0,
    )

    if fx_score >= 0.70:
        zt_label = "High FX Stress / Dollar Dominance"
    elif fx_score >= 0.45:
        zt_label = "Balanced FX Pressure"
    else:
        zt_label = "Low-Conviction / Monitoring Regime"

    final_metric = round(float(fx_score) * 100, 2)

    compartment = {
        "component": "GeoScen FX Compartment",
        "version": "v1",
        "built_at_utc": datetime.now(timezone.utc).isoformat(),

        "compartment": "fx",
        "deployment_role": "currency_pressure_and_global_liquidity_compartment",

        "zt": {
            "label": zt_label,
            "fx_pressure_score": fx_score,
            "final_metric_0_100": final_metric,
        },

        "fx_context": {
            "fx_serving_available": fx_serving.get("available"),
            "fx_zt_available": fx_zt.get("available"),
            "fx_price_data_available": fx_price_data.get("available"),
            "fx_sigma_data_available": fx_sigma_data.get("available"),
            "fx_spreads_data_available": fx_spreads_data.get("available"),
            "frontend_available": frontend.get("available"),
            "contradiction_score": contradiction_score,
        },

        "graph_area": [
            "fx_latest",
            "usd_dxy_panel",
            "eurusd_panel",
            "usdjpy_panel",
            "contradiction_engine",
            "geoscen_frontend_overlay",
        ],

        "rbl": {
            "text": (
                f"FX reads as {zt_label}. "
                f"FX pressure score is currently {fx_score}. "
                f"The FX compartment should be interpreted as a global liquidity and cross-border stress layer, "
                f"with USD, EUR/USD, and USD/JPY acting as primary routing signals."
            ),
        },

        "readiness": readiness,
        "source_rows": source_rows,

        "frontend_contract": {
            "panel_title": "FX",
            "primary_fields": [
                "Zₜ",
                "FX Pressure",
                "USD / DXY",
                "EUR/USD",
                "USD/JPY",
                "Contradiction Overlay",
                "Final Metric",
            ],
            "deployable": readiness["readiness_state"] in {"ready", "degraded_ready"},
        },

        "governance": {
            "compartmentalized": True,
            "cross_asset_required": True,
            "global_liquidity_layer": True,
            "currency_stress_tracking": True,
            "source_provenance_required": True,
            "deploy_all_together_later": True,
            "ai_last": True,
        },
    }

    out_json = OUT_DIR / "geoscen_fx_compartment_v1.json"
    out_txt = OUT_DIR / "geoscen_fx_compartment_v1.txt"

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(compartment, f, indent=2, default=str)

    with open(out_txt, "w", encoding="utf-8-sig") as f:
        f.write("GEOSCEN FX COMPARTMENT V1\n")
        f.write("=" * 60 + "\n\n")

        f.write(f"zt_label: {compartment['zt']['label']}\n")
        f.write(f"fx_pressure_score: {compartment['zt']['fx_pressure_score']}\n")
        f.write(f"final_metric_0_100: {compartment['zt']['final_metric_0_100']}\n\n")

        f.write("FX CONTEXT\n")
        f.write("-" * 60 + "\n")
        for k, v in compartment["fx_context"].items():
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

    print("OK | GeoScen FX Compartment v1 built")
    print(f"zt_label         : {compartment['zt']['label']}")
    print(f"final_metric     : {compartment['zt']['final_metric_0_100']}")
    print(f"readiness_state  : {readiness['readiness_state']}")
    print(f"readiness_ratio  : {readiness['readiness_ratio']}")

    print("\nArtifacts written:")
    print(out_json)
    print(out_txt)


if __name__ == "__main__":
    main()


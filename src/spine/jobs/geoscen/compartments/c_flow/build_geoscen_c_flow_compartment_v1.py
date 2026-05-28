from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd


REPO_ROOT = Path.cwd()

OUT_DIR = REPO_ROOT / "data" / "serving" / "geoscen" / "compartments" / "c_flow"
OUT_DIR.mkdir(parents=True, exist_ok=True)

SOURCE_CONTRACT = {
    "c_flow_latest": REPO_ROOT / "data" / "serving" / "c_flow" / "c_flow_latest_v5.json",
    "wti_pressure": REPO_ROOT / "data" / "serving" / "wti" / "wti_inflation_pressure.json",
    "frontend": REPO_ROOT / "data" / "serving" / "geoscen" / "frontend" / "geoscen_frontend_intelligence_layer_v1.json",
    "breadth": REPO_ROOT / "data" / "serving" / "equities" / "breadth_factor_serving_v1.parquet",
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
    c_flow = read_json(SOURCE_CONTRACT["c_flow_latest"])
    wti = read_json(SOURCE_CONTRACT["wti_pressure"])
    frontend = read_json(SOURCE_CONTRACT["frontend"])
    breadth = read_latest_parquet(SOURCE_CONTRACT["breadth"])
    contradiction = read_json(SOURCE_CONTRACT["contradiction"])

    payloads = {
        "c_flow_latest": c_flow,
        "wti_pressure": wti,
        "frontend": frontend,
        "breadth": breadth,
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

    c_flow_score = first_available(
        c_flow,
        ["fund_flow_pressure", "score", "c_flow_score", "pressure_score"],
        default=0.0,
    )

    breadth_score = first_available(
        breadth,
        ["breadth_factor_score", "score"],
        default=None,
    )

    contradiction_score = first_available(
        contradiction,
        ["contradiction_score"],
        default=0.0,
    )

    if c_flow_score >= 0.70:
        zt_label = "Risk-On Capital Flow Expansion"
    elif c_flow_score >= 0.45:
        zt_label = "Balanced Capital Flow"
    else:
        zt_label = "Defensive / Weak Capital Flow"

    final_metric = round(float(c_flow_score) * 100, 2)

    compartment = {
        "component": "GeoScen C_FLOW Compartment",
        "version": "v1",
        "built_at_utc": datetime.now(timezone.utc).isoformat(),

        "compartment": "c_flow",
        "deployment_role": "capital_flow_liquidity_pressure_compartment",

        "zt": {
            "label": zt_label,
            "c_flow_score": c_flow_score,
            "final_metric_0_100": final_metric,
        },

        "capital_flow_context": {
            "wti_pressure_available": wti.get("available"),
            "breadth_score": breadth_score,
            "contradiction_score": contradiction_score,
            "frontend_available": frontend.get("available"),
        },

        "graph_area": [
            "c_flow_latest_v5",
            "wti_inflation_pressure",
            "breadth_factor_serving_v1",
            "geoscen_frontend_overlay",
            "contradiction_engine",
        ],

        "rbl": {
            "text": (
                f"C_FLOW reads as {zt_label}. "
                f"Fund-flow pressure is currently {c_flow_score}. "
                f"WTI pressure should be interpreted as commodity-pressure context feeding the liquidity channel, "
                f"while equity breadth acts as confirmation or divergence validation for capital rotation."
            ),
        },

        "readiness": readiness,
        "source_rows": source_rows,

        "frontend_contract": {
            "panel_title": "C_FLOW",
            "primary_fields": [
                "Zₜ",
                "Fund Flow Pressure",
                "WTI Pressure",
                "Breadth Confirmation",
                "Contradiction Overlay",
                "Final Metric",
            ],
            "deployable": readiness["readiness_state"] in {"ready", "degraded_ready"},
        },

        "governance": {
            "compartmentalized": True,
            "cross_asset_required": True,
            "wti_context_required": True,
            "breadth_confirmation_required": True,
            "source_provenance_required": True,
            "deploy_all_together_later": True,
            "ai_last": True,
        },
    }

    out_json = OUT_DIR / "geoscen_c_flow_compartment_v1.json"
    out_txt = OUT_DIR / "geoscen_c_flow_compartment_v1.txt"

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(compartment, f, indent=2, default=str)

    with open(out_txt, "w", encoding="utf-8-sig") as f:
        f.write("GEOSCEN C_FLOW COMPARTMENT V1\n")
        f.write("=" * 60 + "\n\n")

        f.write(f"zt_label: {compartment['zt']['label']}\n")
        f.write(f"c_flow_score: {compartment['zt']['c_flow_score']}\n")
        f.write(f"final_metric_0_100: {compartment['zt']['final_metric_0_100']}\n\n")

        f.write("CAPITAL FLOW CONTEXT\n")
        f.write("-" * 60 + "\n")
        for k, v in compartment["capital_flow_context"].items():
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

    print("OK | GeoScen C_FLOW Compartment v1 built")
    print(f"zt_label         : {compartment['zt']['label']}")
    print(f"final_metric     : {compartment['zt']['final_metric_0_100']}")
    print(f"readiness_state  : {readiness['readiness_state']}")
    print(f"readiness_ratio  : {readiness['readiness_ratio']}")

    print("\nArtifacts written:")
    print(out_json)
    print(out_txt)


if __name__ == "__main__":
    main()

    
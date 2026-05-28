from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd


REPO_ROOT = Path.cwd()

OUT_DIR = REPO_ROOT / "data" / "serving" / "geoscen" / "compartments" / "equities_index"
OUT_DIR.mkdir(parents=True, exist_ok=True)

SOURCE_CONTRACT = {
    "breadth": REPO_ROOT / "data" / "serving" / "equities" / "breadth_factor_serving_v1.parquet",
    "equities_serving": REPO_ROOT / "data" / "serving" / "equities" / "equities_serving_v2.parquet",
    "equity_market_regime": REPO_ROOT / "data" / "serving" / "equities" / "equity_market_regime_v1.parquet",
    "us_index_data": REPO_ROOT / "data" / "serving" / "equities" / "us_equity_index_data.json",
    "sigma_rank": REPO_ROOT / "data" / "serving" / "equities" / "equities_sigma_rank.json",
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
    breadth = read_latest_parquet(SOURCE_CONTRACT["breadth"])
    equities_serving = read_latest_parquet(SOURCE_CONTRACT["equities_serving"])
    equity_market_regime = read_latest_parquet(SOURCE_CONTRACT["equity_market_regime"])
    us_index_data = read_json(SOURCE_CONTRACT["us_index_data"])
    sigma_rank = read_json(SOURCE_CONTRACT["sigma_rank"])

    payloads = {
        "breadth": breadth,
        "equities_serving": equities_serving,
        "equity_market_regime": equity_market_regime,
        "us_index_data": us_index_data,
        "sigma_rank": sigma_rank,
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

    breadth_score = first_available(
        breadth,
        ["breadth_factor_score", "score", "value"],
        default=None,
    )

    market_regime = first_available(
        equity_market_regime,
        ["regime", "market_regime", "label", "state"],
        default="Unavailable",
    )

    if market_regime == "Unavailable" and breadth_score is not None:
        if float(breadth_score) >= 0.70:
            market_regime = "Healthy Breadth / Risk-On Participation"
        elif float(breadth_score) >= 0.45:
            market_regime = "Balanced Market Participation"
        else:
            market_regime = "Weak Breadth / Risk-Off Participation"

    sigma_state = first_available(
        sigma_rank,
        ["regime", "label", "state"],
        default="Available" if sigma_rank.get("available") else "Unavailable",
    )

    final_metric = None
    if breadth_score is not None:
        final_metric = round(float(breadth_score) * 100, 2)

    compartment = {
        "component": "GeoScen EQUITIES_INDEX Compartment",
        "version": "v1",
        "built_at_utc": datetime.now(timezone.utc).isoformat(),

        "compartment": "equities_index",
        "deployment_role": "broad_market_risk_appetite_compartment",

        "zt": {
            "label": market_regime,
            "breadth_factor_score": breadth_score,
            "final_metric_0_100": final_metric,
        },

        "market_index_context": {
            "us_index_data_available": us_index_data.get("available"),
            "equities_serving_available": equities_serving.get("available"),
            "equity_market_regime": market_regime,
            "sigma_state": sigma_state,
        },

        "graph_area": [
            "breadth_factor_serving_v1",
            "equities_serving_v2",
            "equity_market_regime_v1",
            "us_equity_index_data",
            "equities_sigma_rank",
        ],

        "rbl": {
            "text": (
                f"EQUITIES_INDEX reads as {market_regime}. "
                f"Broad market participation is anchored by breadth factor score {breadth_score}. "
                f"This compartment should be read as the market-index risk appetite layer, distinct from sector / industry cognition."
            ),
        },

        "readiness": readiness,
        "source_rows": source_rows,

        "frontend_contract": {
            "panel_title": "EQUITIES_INDEX",
            "primary_fields": [
                "Zₜ",
                "Market Regime",
                "Breadth Factor",
                "Index Context",
                "Sigma Rank",
                "Final Metric",
            ],
            "deployable": readiness["readiness_state"] in {"ready", "degraded_ready"},
        },

        "governance": {
            "compartmentalized": True,
            "separate_from_equities_sector": True,
            "market_indexes_only": True,
            "source_provenance_required": True,
            "deploy_all_together_later": True,
            "ai_last": True,
        },
    }

    out_json = OUT_DIR / "geoscen_equities_index_compartment_v1.json"
    out_txt = OUT_DIR / "geoscen_equities_index_compartment_v1.txt"

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(compartment, f, indent=2, default=str)

    with open(out_txt, "w", encoding="utf-8-sig") as f:
        f.write("GEOSCEN EQUITIES_INDEX COMPARTMENT V1\n")
        f.write("=" * 60 + "\n\n")

        f.write(f"zt_label: {compartment['zt']['label']}\n")
        f.write(f"breadth_factor_score: {compartment['zt']['breadth_factor_score']}\n")
        f.write(f"final_metric_0_100: {compartment['zt']['final_metric_0_100']}\n\n")

        f.write("MARKET INDEX CONTEXT\n")
        f.write("-" * 60 + "\n")
        for k, v in compartment["market_index_context"].items():
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



    print("OK | GeoScen EQUITIES_INDEX Compartment v1 built")
    print(f"zt_label         : {compartment['zt']['label']}")
    print(f"final_metric     : {compartment['zt']['final_metric_0_100']}")
    print(f"readiness_state  : {readiness['readiness_state']}")
    print(f"readiness_ratio  : {readiness['readiness_ratio']}")

    print("\nArtifacts written:")
    print(out_json)
    print(out_txt)


if __name__ == "__main__":
    main()
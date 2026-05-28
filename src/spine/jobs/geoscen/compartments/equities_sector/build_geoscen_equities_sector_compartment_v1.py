from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd


REPO_ROOT = Path.cwd()

OUT_DIR = REPO_ROOT / "data" / "serving" / "geoscen" / "compartments" / "equities_sector"
OUT_DIR.mkdir(parents=True, exist_ok=True)

SOURCE_CONTRACT = {
    "industry_panel": REPO_ROOT / "data" / "serving" / "equities" / "industry_panel_serving.json",
    "etf_pmi_composite": REPO_ROOT / "data" / "serving" / "equities" / "etf_pmi_composite.json",
    "etf_pmi_breadth": REPO_ROOT / "data" / "serving" / "equities" / "etf_pmi_breadth_by_etf.json",
    "sector_rules": REPO_ROOT / "data" / "serving" / "equities" / "equities_sector_group_rules.parquet",
    "equities_sector_html": REPO_ROOT / "data" / "serving" / "equities" / "equities_industry_sectors_panel_v1.html",
    "sigma_rank": REPO_ROOT / "data" / "serving" / "equities" / "equities_sigma_rank.json",
}


def read_json(path: Path) -> dict:
    if not path.exists():
        return {"available": False, "path": str(path)}

    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)

    return {
        "available": True,
        "path": str(path),
        "payload": obj,
    }


def read_latest_parquet(path: Path) -> dict:
    if not path.exists():
        return {"available": False, "path": str(path)}

    df = pd.read_parquet(path)

    if df.empty:
        return {"available": False, "path": str(path), "reason": "empty"}

    return {
        "available": True,
        "path": str(path),
        "row_count": int(len(df)),
        "columns": df.columns.tolist(),
    }


def file_status(path: Path) -> dict:
    return {
        "available": path.exists(),
        "path": str(path),
    }


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


def extract_count(obj: dict) -> int | None:
    payload = obj.get("payload")

    if isinstance(payload, list):
        return len(payload)

    if isinstance(payload, dict):
        for key in ["count", "signal_count", "industry_count", "sector_count"]:
            if key in payload:
                return payload[key]
        return len(payload.keys())

    return None


def main() -> None:
    industry_panel = read_json(SOURCE_CONTRACT["industry_panel"])
    etf_pmi_composite = read_json(SOURCE_CONTRACT["etf_pmi_composite"])
    etf_pmi_breadth = read_json(SOURCE_CONTRACT["etf_pmi_breadth"])
    sector_rules = read_latest_parquet(SOURCE_CONTRACT["sector_rules"])
    sector_html = file_status(SOURCE_CONTRACT["equities_sector_html"])
    sigma_rank = read_json(SOURCE_CONTRACT["sigma_rank"])

    payloads = {
        "industry_panel": industry_panel,
        "etf_pmi_composite": etf_pmi_composite,
        "etf_pmi_breadth": etf_pmi_breadth,
        "sector_rules": sector_rules,
        "equities_sector_html": sector_html,
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

    industry_count = extract_count(industry_panel)
    pmi_composite_count = extract_count(etf_pmi_composite)
    pmi_breadth_count = extract_count(etf_pmi_breadth)

    sector_rule_count = sector_rules.get("row_count")

    final_metric = round(readiness["readiness_ratio"] * 100, 2)

    sector_state = (
        "Sector / Industry Cognition Ready"
        if readiness["readiness_state"] == "ready"
        else "Sector / Industry Cognition Degraded"
    )

    compartment = {
        "component": "GeoScen EQUITIES_SECTOR Compartment",
        "version": "v1",
        "built_at_utc": datetime.now(timezone.utc).isoformat(),

        "compartment": "equities_sector",
        "deployment_role": "industry_sector_pmi_breadth_cognition_compartment",

        "zt": {
            "label": sector_state,
            "final_metric_0_100": final_metric,
        },

        "sector_context": {
            "industry_panel_available": industry_panel.get("available"),
            "industry_count": industry_count,
            "etf_pmi_composite_available": etf_pmi_composite.get("available"),
            "pmi_composite_count": pmi_composite_count,
            "etf_pmi_breadth_available": etf_pmi_breadth.get("available"),
            "pmi_breadth_count": pmi_breadth_count,
            "sector_rule_count": sector_rule_count,
            "sector_html_available": sector_html.get("available"),
            "sigma_rank_available": sigma_rank.get("available"),
        },

        "graph_area": [
            "industry_panel_serving",
            "etf_pmi_composite",
            "etf_pmi_breadth_by_etf",
            "equities_sector_group_rules",
            "equities_industry_sectors_panel",
            "equities_sigma_rank",
        ],

        "rbl": {
            "text": (
                f"EQUITIES_SECTOR reads as {sector_state}. "
                f"This compartment separates industry / sector cognition from broad market-index behavior. "
                f"It is designed to consume PMI-linked ETF mappings, sector group rules, industry panel serving, "
                f"and sector breadth context for cyclical versus defensive rotation analysis."
            ),
        },

        "readiness": readiness,
        "source_rows": source_rows,

        "frontend_contract": {
            "panel_title": "EQUITIES_SECTOR",
            "primary_fields": [
                "Zₜ",
                "Industry Panel",
                "ETF PMI Composite",
                "ETF PMI Breadth",
                "Sector Rules",
                "Sector Rotation",
                "Final Metric",
            ],
            "deployable": readiness["readiness_state"] in {"ready", "degraded_ready"},
        },

        "governance": {
            "compartmentalized": True,
            "separate_from_equities_index": True,
            "industry_sector_only": True,
            "pmi_linkage_required": True,
            "source_provenance_required": True,
            "deploy_all_together_later": True,
            "ai_last": True,
        },
    }

    out_json = OUT_DIR / "geoscen_equities_sector_compartment_v1.json"
    out_txt = OUT_DIR / "geoscen_equities_sector_compartment_v1.txt"

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(compartment, f, indent=2, default=str)

    with open(out_txt, "w", encoding="utf-8-sig") as f:
        f.write("GEOSCEN EQUITIES_SECTOR COMPARTMENT V1\n")
        f.write("=" * 60 + "\n\n")

        f.write(f"zt_label: {compartment['zt']['label']}\n")
        f.write(f"final_metric_0_100: {compartment['zt']['final_metric_0_100']}\n\n")

        f.write("SECTOR CONTEXT\n")
        f.write("-" * 60 + "\n")
        for k, v in compartment["sector_context"].items():
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

    print("OK | GeoScen EQUITIES_SECTOR Compartment v1 built")
    print(f"zt_label         : {compartment['zt']['label']}")
    print(f"final_metric     : {compartment['zt']['final_metric_0_100']}")
    print(f"readiness_state  : {readiness['readiness_state']}")
    print(f"readiness_ratio  : {readiness['readiness_ratio']}")

    print("\nArtifacts written:")
    print(out_json)
    print(out_txt)


if __name__ == "__main__":
    main()
    
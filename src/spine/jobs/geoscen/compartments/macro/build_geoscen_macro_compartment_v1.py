from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timezone


REPO_ROOT = Path.cwd()

OUT_DIR = REPO_ROOT / "data" / "serving" / "geoscen" / "compartments" / "macro"
OUT_DIR.mkdir(parents=True, exist_ok=True)

SOURCE_CONTRACT = {
    "frontend": REPO_ROOT / "data" / "serving" / "geoscen" / "frontend" / "geoscen_frontend_intelligence_layer_v1.json",
    "rbl": REPO_ROOT / "data" / "serving" / "geoscen" / "geoscen_rbl_synthesis_v1.json",
    "structural": REPO_ROOT / "data" / "serving" / "geoscen" / "structure" / "geoscen_structural_macro_layer_v1.json",
    "macro_registry": REPO_ROOT / "data" / "serving" / "geoscen" / "macro" / "geoscen_macro_registry_v1.json",
    "macro_ingestion": REPO_ROOT / "data" / "serving" / "geoscen" / "macro" / "geoscen_macro_ingestion_v1.json",
    "drift": REPO_ROOT / "data" / "serving" / "geoscen" / "drift" / "geoscen_historical_narrative_drift_engine_v1.json",
    "contradiction": REPO_ROOT / "data" / "serving" / "geoscen" / "contradiction" / "geoscen_contradiction_engine_v1.json",
    "cb_cognition": REPO_ROOT / "data" / "serving" / "geoscen" / "cb" / "geoscen_cross_country_policy_cognition_v1.json",
}


def read_json(path: Path) -> dict:
    if not path.exists():
        return {
            "available": False,
            "path": str(path),
        }

    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)

    if isinstance(obj, dict):
        obj["available"] = True

    return obj


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


def main() -> None:
    payloads = {
        name: read_json(path)
        for name, path in SOURCE_CONTRACT.items()
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

    frontend = payloads["frontend"]
    rbl = payloads["rbl"]
    structural = payloads["structural"]
    ingestion = payloads["macro_ingestion"]
    drift = payloads["drift"]
    contradiction = payloads["contradiction"]
    cb_cognition = payloads["cb_cognition"]

    temperature_score = frontend.get("temperature_score")
    final_metric = None

    if temperature_score is not None:
        final_metric = round(float(temperature_score) * 100, 2)

    macro_compartment = {
        "component": "GeoScen MACRO Compartment",
        "version": "v1",
        "built_at_utc": datetime.now(timezone.utc).isoformat(),

        "compartment": "macro",
        "deployment_role": "primary_macro_cognition_compartment",

        "zt": {
            "temperature": frontend.get("temperature") or rbl.get("temperature"),
            "temperature_score": temperature_score,
            "final_metric_0_100": final_metric,
        },

        "rbl": {
            "text": (
            frontend.get("rbl")
            or frontend.get("evidence", {}).get("geoscen_rbl", {}).get("rbl")
            or rbl.get("rbl")
            or rbl.get("rbl_text")
            or rbl.get("summary")
            or rbl.get("report")
        ),
            "source": "geoscen_frontend_intelligence_layer_v1 / geoscen_rbl_synthesis_v1",
        },

        "structural_macro": {
            "regime": structural.get("structural_regime"),
            "pressure_score": structural.get("structural_pressure_score"),
            "signal_family_count": structural.get("signal_family_count"),
        },

        "macro_registry": {
            "signal_count": payloads["macro_registry"].get("signal_count"),
            "free_signal_count": payloads["macro_registry"].get("free_signal_count"),
            "geoscen_ready_count": payloads["macro_registry"].get("geoscen_ready_count"),
        },

        "macro_ingestion": {
            "signal_count": ingestion.get("signal_count"),
            "fred_ready_count": ingestion.get("fred_ready_count"),
            "ingestion_ready_count": ingestion.get("ingestion_ready_count"),
            "external_pending_count": ingestion.get("external_pending_count"),
            "mapped_later_count": ingestion.get("mapped_later_count"),
        },

        "narrative_drift": {
            "drift_score": drift.get("drift_score"),
            "drift_regime": drift.get("drift_regime"),
            "active_drift_count": drift.get("active_drift_count"),
        },

        "contradiction": {
            "contradiction_score": contradiction.get("contradiction_score"),
            "contradiction_severity": contradiction.get("contradiction_severity"),
            "active_contradiction_count": contradiction.get("active_contradiction_count"),
        },

        "cb_cognition": {
            "active_cb_count": cb_cognition.get("active_cb_count"),
            "tracked_cb_count": cb_cognition.get("tracked_cb_count"),
            "oraclechambers_ready": cb_cognition.get("oraclechambers_ready"),
        },

        "readiness": readiness,
        "source_rows": source_rows,

        "frontend_contract": {
            "panel_title": "MACRO",
            "primary_fields": [
                "Zₜ",
                "RBL",
                "Structural Macro",
                "Narrative Drift",
                "Contradiction",
                "CB Cognition",
                "Final Metric",
            ],
            "deployable": readiness["readiness_state"] in {"ready", "degraded_ready"},
        },

        "governance": {
            "compartmentalized": True,
            "offline_validated": True,
            "source_provenance_required": True,
            "deploy_all_together_later": True,
            "cloud_not_required": True,
            "ai_last": True,
        },
    }

    out_json = OUT_DIR / "geoscen_macro_compartment_v1.json"
    out_txt = OUT_DIR / "geoscen_macro_compartment_v1.txt"

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(macro_compartment, f, indent=2, default=str)

    with open(out_txt, "w", encoding="utf-8-sig") as f:
        f.write("GEOSCEN MACRO COMPARTMENT V1\n")
        f.write("=" * 60 + "\n\n")

        f.write(f"temperature: {macro_compartment['zt']['temperature']}\n")
        f.write(f"temperature_score: {macro_compartment['zt']['temperature_score']}\n")
        f.write(f"final_metric_0_100: {macro_compartment['zt']['final_metric_0_100']}\n\n")

        f.write("STRUCTURAL MACRO\n")
        f.write("-" * 60 + "\n")
        f.write(f"regime: {macro_compartment['structural_macro']['regime']}\n")
        f.write(f"pressure_score: {macro_compartment['structural_macro']['pressure_score']}\n")
        f.write(f"signal_family_count: {macro_compartment['structural_macro']['signal_family_count']}\n\n")

        f.write("MACRO INGESTION\n")
        f.write("-" * 60 + "\n")
        for k, v in macro_compartment["macro_ingestion"].items():
            f.write(f"{k}: {v}\n")

        f.write("\nNARRATIVE DRIFT\n")
        f.write("-" * 60 + "\n")
        for k, v in macro_compartment["narrative_drift"].items():
            f.write(f"{k}: {v}\n")

        f.write("\nCONTRADICTION\n")
        f.write("-" * 60 + "\n")
        for k, v in macro_compartment["contradiction"].items():
            f.write(f"{k}: {v}\n")

        f.write("\nCB COGNITION\n")
        f.write("-" * 60 + "\n")
        for k, v in macro_compartment["cb_cognition"].items():
            f.write(f"{k}: {v}\n")

        f.write("\nREADINESS\n")
        f.write("-" * 60 + "\n")
        for k, v in readiness.items():
            f.write(f"{k}: {v}\n")

        f.write("\nRBL\n")
        f.write("-" * 60 + "\n")
        f.write((macro_compartment["rbl"]["text"] or "Unavailable") + "\n")

    print("OK | GeoScen MACRO Compartment v1 built")
    print(f"temperature      : {macro_compartment['zt']['temperature']}")
    print(f"final_metric     : {macro_compartment['zt']['final_metric_0_100']}")
    print(f"readiness_state  : {readiness['readiness_state']}")
    print(f"readiness_ratio  : {readiness['readiness_ratio']}")

    print("\nArtifacts written:")
    print(out_json)
    print(out_txt)


if __name__ == "__main__":
    main()

from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timezone


REPO_ROOT = Path.cwd()

OUT_DIR = REPO_ROOT / "data" / "serving" / "geoscen" / "compartments"
OUT_DIR.mkdir(parents=True, exist_ok=True)

SOURCE_CONTRACT = {
    "macro": REPO_ROOT / "data" / "serving" / "geoscen" / "compartments" / "macro" / "geoscen_macro_compartment_v1.json",
    "equities_index": REPO_ROOT / "data" / "serving" / "geoscen" / "compartments" / "equities_index" / "geoscen_equities_index_compartment_v1.json",
    "equities_sector": REPO_ROOT / "data" / "serving" / "geoscen" / "compartments" / "equities_sector" / "geoscen_equities_sector_compartment_v1.json",
    "c_flow": REPO_ROOT / "data" / "serving" / "geoscen" / "compartments" / "c_flow" / "geoscen_c_flow_compartment_v1.json",
    "fx": REPO_ROOT / "data" / "serving" / "geoscen" / "compartments" / "fx" / "geoscen_fx_compartment_v1.json",
    "rates": REPO_ROOT / "data" / "serving" / "geoscen" / "compartments" / "rates" / "geoscen_rates_compartment_v1.json",
}


def read_json(path: Path) -> dict:
    if not path.exists():
        return {"available": False, "path": str(path)}

    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)

    if isinstance(obj, dict):
        obj["available"] = True

    return obj


def get_readiness_state(payload: dict) -> str:
    return (
        payload.get("readiness", {}).get("readiness_state")
        or "missing"
    )


def get_final_metric(payload: dict):
    return (
        payload.get("zt", {}).get("final_metric_0_100")
        or payload.get("zt", {}).get("final_metric")
    )


def main() -> None:
    compartments = {
        name: read_json(path)
        for name, path in SOURCE_CONTRACT.items()
    }

    rows = []

    for name, payload in compartments.items():
        readiness_state = get_readiness_state(payload)

        deployable = (
            payload.get("frontend_contract", {}).get("deployable", False)
        )

        rows.append({
            "compartment": name,
            "available": payload.get("available", False),
            "readiness_state": readiness_state,
            "deployable": deployable,
            "final_metric_0_100": get_final_metric(payload),
            "component": payload.get("component"),
            "path": str(SOURCE_CONTRACT[name]),
        })

    ready_count = sum(
        1 for row in rows
        if row["readiness_state"] in {"ready", "degraded_ready"}
    )

    deployable_count = sum(
        1 for row in rows
        if row["deployable"]
    )

    available_count = sum(
        1 for row in rows
        if row["available"]
    )

    total = len(rows)

    readiness_ratio = round(
        ready_count / max(1, total),
        4,
    )

    deployable_ratio = round(
        deployable_count / max(1, total),
        4,
    )

    available_ratio = round(
        available_count / max(1, total),
        4,
    )

    deployment_ready = (
        available_count == total
        and ready_count == total
        and deployable_count == total
    )

    payload = {
        "component": "GeoScen Cross-Compartment Orchestrator",
        "version": "v1",
        "built_at_utc": datetime.now(timezone.utc).isoformat(),

        "deployment_ready": deployment_ready,

        "compartment_count": total,
        "available_count": available_count,
        "ready_count": ready_count,
        "deployable_count": deployable_count,

        "available_ratio": available_ratio,
        "readiness_ratio": readiness_ratio,
        "deployable_ratio": deployable_ratio,

        "compartment_rows": rows,

        "deployment_sequence": [
            "macro",
            "equities_index",
            "equities_sector",
            "c_flow",
            "fx",
            "rates",
        ],

        "unified_frontend_contract": {
            "panels": [
                "MACRO",
                "EQUITIES_INDEX",
                "EQUITIES_SECTOR",
                "C_FLOW",
                "FX",
                "RATES",
            ],
            "deploy_all_together": True,
            "offline_validated": True,
            "localhost_ready": True,
        },

        "governance": {
            "compartmentalized": True,
            "cross_domain_sync_required": True,
            "deploy_all_together_later": True,
            "offline_first": True,
            "source_provenance_required": True,
            "ai_last": True,
        },
    }

    out_json = (
        OUT_DIR
        / "geoscen_compartment_orchestrator_v1.json"
    )

    out_txt = (
        OUT_DIR
        / "geoscen_compartment_orchestrator_v1.txt"
    )

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    with open(out_txt, "w", encoding="utf-8-sig") as f:
        f.write("GEOSCEN CROSS-COMPARTMENT ORCHESTRATOR V1\n")
        f.write("=" * 60 + "\n\n")

        f.write(
            f"deployment_ready: {payload['deployment_ready']}\n"
        )

        f.write(
            f"compartment_count: {payload['compartment_count']}\n"
        )

        f.write(
            f"available_count: {payload['available_count']}\n"
        )

        f.write(
            f"ready_count: {payload['ready_count']}\n"
        )

        f.write(
            f"deployable_count: {payload['deployable_count']}\n"
        )

        f.write(
            f"readiness_ratio: {payload['readiness_ratio']}\n"
        )

        f.write(
            f"deployable_ratio: {payload['deployable_ratio']}\n\n"
        )

        f.write("COMPARTMENT ROWS\n")
        f.write("-" * 60 + "\n")

        for row in rows:
            f.write(
                f"- {row['compartment']} | "
                f"available={row['available']} | "
                f"readiness={row['readiness_state']} | "
                f"deployable={row['deployable']} | "
                f"metric={row['final_metric_0_100']}\n"
            )

        f.write("\nDEPLOYMENT SEQUENCE\n")
        f.write("-" * 60 + "\n")

        for item in payload["deployment_sequence"]:
            f.write(f"- {item}\n")

    print("OK | GeoScen Cross-Compartment Orchestrator v1 built")
    print(f"deployment_ready : {payload['deployment_ready']}")
    print(f"readiness_ratio  : {payload['readiness_ratio']}")
    print(f"deployable_ratio : {payload['deployable_ratio']}")

    print("\nArtifacts written:")
    print(out_json)
    print(out_txt)


if __name__ == "__main__":
    main()

    
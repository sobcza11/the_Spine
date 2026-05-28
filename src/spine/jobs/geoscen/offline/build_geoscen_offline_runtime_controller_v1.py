from __future__ import annotations

import json
import shutil
from pathlib import Path
from datetime import datetime, timezone


REPO_ROOT = Path.cwd()

OUT_DIR = REPO_ROOT / "data" / "serving" / "geoscen" / "offline"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OFFLINE_SITE_DIR = REPO_ROOT / "_offline_site"
OFFLINE_SITE_DIR.mkdir(parents=True, exist_ok=True)


SOURCE_CONTRACT = {
    "frontend": REPO_ROOT / "data" / "serving" / "geoscen" / "frontend" / "geoscen_frontend_intelligence_layer_v1.json",
    "contradiction": REPO_ROOT / "data" / "serving" / "geoscen" / "contradiction" / "geoscen_contradiction_engine_v1.json",
    "drift": REPO_ROOT / "data" / "serving" / "geoscen" / "drift" / "geoscen_historical_narrative_drift_engine_v1.json",
    "cb_cognition": REPO_ROOT / "data" / "serving" / "geoscen" / "cb" / "geoscen_cross_country_policy_cognition_v1.json",
    "macro_registry": REPO_ROOT / "data" / "serving" / "geoscen" / "macro" / "geoscen_macro_registry_v1.json",
    "macro_ingestion": REPO_ROOT / "data" / "serving" / "geoscen" / "macro" / "geoscen_macro_ingestion_v1.json",
    "structural": REPO_ROOT / "data" / "serving" / "geoscen" / "structure" / "geoscen_structural_macro_layer_v1.json",
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


def build_runtime_rows() -> list[dict]:
    rows = []

    for component, path in SOURCE_CONTRACT.items():
        exists = path.exists()

        rows.append({
            "component": component,
            "path": str(path),
            "available": exists,
            "runtime_status": "ready" if exists else "missing",
        })

    return rows


def sync_offline_site(runtime_rows: list[dict]) -> list[dict]:
    synced = []

    geoscen_target = OFFLINE_SITE_DIR / "geoscen_runtime"
    geoscen_target.mkdir(parents=True, exist_ok=True)

    for row in runtime_rows:
        if not row["available"]:
            continue

        src = Path(row["path"])

        target = geoscen_target / src.name

        shutil.copy2(src, target)

        synced.append({
            "component": row["component"],
            "target": str(target),
            "sync_status": "synced",
        })

    return synced


def build_health(runtime_rows: list[dict]) -> dict:
    total = len(runtime_rows)

    ready = sum(
        1 for row in runtime_rows
        if row["runtime_status"] == "ready"
    )

    ratio = round(ready / max(1, total), 4)

    if ratio >= 0.95:
        state = "healthy"
    elif ratio >= 0.75:
        state = "degraded"
    else:
        state = "critical"

    return {
        "component_count": total,
        "ready_count": ready,
        "health_ratio": ratio,
        "runtime_state": state,
    }


def main() -> None:
    runtime_rows = build_runtime_rows()

    sync_rows = sync_offline_site(runtime_rows)

    health = build_health(runtime_rows)

    payload = {
        "component": "GeoScen Offline Runtime Controller",
        "version": "v1",
        "built_at_utc": datetime.now(timezone.utc).isoformat(),

        "localhost_target": "http://localhost:8787/",
        "offline_mode": True,

        "runtime_rows": runtime_rows,
        "sync_rows": sync_rows,
        "health": health,

        "runtime_ready": health["runtime_state"] in {"healthy", "degraded"},

        "governance": {
            "offline_first": True,
            "registry_driven": True,
            "source_provenance_required": True,
            "localhost_ready": True,
            "cloud_not_required": True,
            "ai_last": True,
        },
    }

    out_json = OUT_DIR / "geoscen_offline_runtime_controller_v1.json"
    out_txt = OUT_DIR / "geoscen_offline_runtime_controller_v1.txt"

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    with open(out_txt, "w", encoding="utf-8-sig") as f:
        f.write("GEOSCEN OFFLINE RUNTIME CONTROLLER V1\n")
        f.write("=" * 60 + "\n\n")

        f.write(f"localhost_target: {payload['localhost_target']}\n")
        f.write(f"offline_mode: {payload['offline_mode']}\n")
        f.write(f"runtime_ready: {payload['runtime_ready']}\n\n")

        f.write("HEALTH\n")
        f.write("-" * 60 + "\n")

        for k, v in health.items():
            f.write(f"- {k}: {v}\n")

        f.write("\nRUNTIME ROWS\n")
        f.write("-" * 60 + "\n")

        for row in runtime_rows:
            f.write(
                f"- {row['component']} | "
                f"status={row['runtime_status']} | "
                f"available={row['available']}\n"
            )

        f.write("\nSYNC ROWS\n")
        f.write("-" * 60 + "\n")

        for row in sync_rows:
            f.write(
                f"- {row['component']} | "
                f"{row['sync_status']} | "
                f"{row['target']}\n"
            )

    print("OK | GeoScen Offline Runtime Controller v1 built")
    print(f"runtime_state : {health['runtime_state']}")
    print(f"health_ratio  : {health['health_ratio']}")
    print(f"runtime_ready : {payload['runtime_ready']}")

    print("\nArtifacts written:")
    print(out_json)
    print(out_txt)


if __name__ == "__main__":
    main()
    
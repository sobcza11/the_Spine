from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd


REPO_ROOT = Path.cwd()

REGISTRY_PATH = REPO_ROOT / "data" / "serving" / "geoscen" / "macro" / "geoscen_macro_registry_v1.parquet"

OUT_DIR = REPO_ROOT / "data" / "serving" / "geoscen" / "macro"
OUT_DIR.mkdir(parents=True, exist_ok=True)


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


def existing_serving_status(source_key: str) -> dict:
    paths = {
        "wti_inflation_pressure": REPO_ROOT / "data" / "serving" / "wti" / "wti_inflation_pressure.json",
        "rates_zt_latest": REPO_ROOT / "data" / "serving" / "rates" / "rates_zt_latest.json",
        "breadth_factor_serving_v1": REPO_ROOT / "data" / "serving" / "equities" / "breadth_factor_serving_v1.parquet",
        "c_flow_latest_v5": REPO_ROOT / "data" / "serving" / "c_flow" / "c_flow_latest_v5.json",
        "geoscen_contradiction_engine_v1": REPO_ROOT / "data" / "serving" / "geoscen" / "contradiction" / "geoscen_contradiction_engine_v1.json",
        "geoscen_historical_narrative_drift_engine_v1": REPO_ROOT / "data" / "serving" / "geoscen" / "drift" / "geoscen_historical_narrative_drift_engine_v1.json",
        "geoscen_cross_country_policy_cognition_v1": REPO_ROOT / "data" / "serving" / "geoscen" / "cb" / "geoscen_cross_country_policy_cognition_v1.json",
    }

    path = paths.get(source_key)

    if path is None:
        return {
            "ingestion_status": "mapped_later",
            "available": False,
            "path": None,
        }

    return {
        "ingestion_status": "available" if path.exists() else "missing",
        "available": path.exists(),
        "path": str(path),
    }


def classify_ingestion(row: dict) -> dict:
    route = row["ingestion_route"]
    source_key = row["source_key"]

    if route == "fred":
        return {
            "ingestion_status": "fred_ready",
            "available": True,
            "method": "fred_api_or_pandas_datareader",
            "source_key": source_key,
        }

    if route == "existing_serving":
        status = existing_serving_status(source_key)
        status["method"] = "existing_serving_layer"
        return status

    if route == "derived":
        return {
            "ingestion_status": "derived_ready",
            "available": True,
            "method": "internal_derivation",
            "source_key": source_key,
        }

    return {
        "ingestion_status": "external_pending",
        "available": False,
        "method": route,
        "source_key": source_key,
    }


def main() -> None:
    if not REGISTRY_PATH.exists():
        raise FileNotFoundError(f"Missing registry: {REGISTRY_PATH}")

    registry = pd.read_parquet(REGISTRY_PATH).copy()

    rows = []

    for row in registry.to_dict("records"):
        ingest = classify_ingestion(row)

        rows.append({
            **row,
            "ingestion_status": ingest.get("ingestion_status"),
            "ingestion_available": ingest.get("available"),
            "ingestion_method": ingest.get("method"),
            "resolved_path": ingest.get("path"),
            "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        })

    df = pd.DataFrame(rows)

    payload = {
        "component": "GeoScen Macro Ingestion Contract",
        "version": "v1",
        "built_at_utc": datetime.now(timezone.utc).isoformat(),
        "signal_count": int(len(df)),
        "fred_ready_count": int((df["ingestion_status"] == "fred_ready").sum()),
        "existing_available_count": int((df["ingestion_status"] == "available").sum()),
        "derived_ready_count": int((df["ingestion_status"] == "derived_ready").sum()),
        "external_pending_count": int((df["ingestion_status"] == "external_pending").sum()),
        "mapped_later_count": int((df["ingestion_status"] == "mapped_later").sum()),
        "missing_existing_count": int((df["ingestion_status"] == "missing").sum()),
        "ingestion_ready_count": int(df["ingestion_available"].sum()),
        "governance": {
            "registry_driven": True,
            "source_provenance_required": True,
            "ai_last": True,
            "fred_first": True,
            "existing_serving_reused": True,
            "external_sources_not_forced": True,
        },
    }

    out_json = OUT_DIR / "geoscen_macro_ingestion_v1.json"
    out_txt = OUT_DIR / "geoscen_macro_ingestion_v1.txt"
    out_parquet = OUT_DIR / "geoscen_macro_ingestion_v1.parquet"
    out_csv = OUT_DIR / "geoscen_macro_ingestion_v1.csv"

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    df.to_parquet(out_parquet, index=False)
    df.to_csv(out_csv, index=False)

    with open(out_txt, "w", encoding="utf-8-sig") as f:
        f.write("GEOSCEN MACRO INGESTION V1\n")
        f.write("=" * 60 + "\n\n")
        f.write(f"signal_count: {payload['signal_count']}\n")
        f.write(f"fred_ready_count: {payload['fred_ready_count']}\n")
        f.write(f"existing_available_count: {payload['existing_available_count']}\n")
        f.write(f"derived_ready_count: {payload['derived_ready_count']}\n")
        f.write(f"external_pending_count: {payload['external_pending_count']}\n")
        f.write(f"mapped_later_count: {payload['mapped_later_count']}\n")
        f.write(f"missing_existing_count: {payload['missing_existing_count']}\n")
        f.write(f"ingestion_ready_count: {payload['ingestion_ready_count']}\n\n")

        f.write("INGESTION ROWS\n")
        f.write("-" * 60 + "\n")

        for row in df.to_dict("records"):
            f.write(
                f"- {row['name']} | "
                f"{row['section']} | "
                f"{row['source_key']} | "
                f"route={row['ingestion_route']} | "
                f"status={row['ingestion_status']} | "
                f"available={row['ingestion_available']}\n"
            )

    print("OK | GeoScen Macro Ingestion v1 built")
    print(f"signal_count              : {payload['signal_count']}")
    print(f"fred_ready_count          : {payload['fred_ready_count']}")
    print(f"existing_available_count  : {payload['existing_available_count']}")
    print(f"derived_ready_count       : {payload['derived_ready_count']}")
    print(f"ingestion_ready_count     : {payload['ingestion_ready_count']}")

    print("\nArtifacts written:")
    print(out_json)
    print(out_txt)
    print(out_parquet)
    print(out_csv)


if __name__ == "__main__":
    main()


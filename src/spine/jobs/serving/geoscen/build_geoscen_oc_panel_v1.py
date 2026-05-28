from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


REPO_ROOT = Path.cwd()

QUALAYER_PACKET_PATH = REPO_ROOT / "data" / "llm" / "qualayer" / "qualayer_packet_v1.json"
QUALAYER_VALIDATION_PATH = REPO_ROOT / "data" / "llm" / "qualayer" / "qualayer_validation_v1.json"
OC_BRIEF_PATH = REPO_ROOT / "data" / "llm" / "oraclechambers" / "oraclechambers_brief_v1.md"
OC_VALIDATION_PATH = REPO_ROOT / "data" / "llm" / "oraclechambers" / "oraclechambers_validation_v1.json"

ZT_PATH = REPO_ROOT / "data" / "serving" / "geoscen" / "zt_v1.parquet"

OUTPUT_PATH = REPO_ROOT / "data" / "serving" / "geoscen" / "geoscen_oc_panel_v1.json"


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def load_zt() -> dict[str, Any]:
    if not ZT_PATH.exists():
        return {"status": "missing"}

    df = pd.read_parquet(ZT_PATH)

    df = df.sort_values("date")
    row = df.iloc[-1]

    return {
        "status": "active",
        "date": row["date"].strftime("%Y-%m-%d"),
        "zt_value": float(row["zt_v1"]),
        "components": {
            "cb_norm": float(row["cb_norm"]),
            "pmi_norm": float(row["pmi_norm"]),
            "rates_norm": float(row["rates_norm"]),
        }
    }


def load_zt_timeseries() -> list[dict[str, Any]]:
    if not ZT_PATH.exists():
        return []

    df = pd.read_parquet(ZT_PATH).sort_values("date")

    return [
        {
            "date": row["date"].strftime("%Y-%m-%d"),
            "zt": float(row["zt_v1"])
        }
        for _, row in df.tail(50).iterrows()
    ]


def build_panel():
    qualayer_packet = load_json(QUALAYER_PACKET_PATH)
    qualayer_validation = load_json(QUALAYER_VALIDATION_PATH)
    oraclechambers_brief = load_text(OC_BRIEF_PATH)
    oraclechambers_validation = load_json(OC_VALIDATION_PATH)

    zt = load_zt()
    zt_series = load_zt_timeseries()

    evidence = qualayer_packet["evidence"]

    statuses = [
        qualayer_validation.get("status"),
        oraclechambers_validation.get("status")
    ]

    if "FAIL" in statuses:
        panel_status = "FAIL"
    elif "WARN" in statuses:
        panel_status = "WARN"
    else:
        panel_status = "PASS"

    return {
        "panel_version": "v1",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": panel_status,

        "layout": {
            "title": "GEOSCEN | OC",

            "top_left": {
                "name": "Z_t",
                **zt
            },

            "top_right": {
                "name": "Graph Area",
                "zt_timeseries": zt_series
            },

            "bottom_left": {
                "name": "OracleChambers",
                "brief": oraclechambers_brief
            },

            "bottom_right": {
                "name": "Reserved",
                "status": "empty"
            }
        },

        "signals": evidence,

        "validation": {
            "qualayer": qualayer_validation,
            "oraclechambers": oraclechambers_validation
        }
    }


def main():
    panel = build_panel()

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(panel, f, indent=2)

    print(f"GeoScen OC panel written: {OUTPUT_PATH}")
    print(f"Panel status: {panel['status']}")


if __name__ == "__main__":
    main()

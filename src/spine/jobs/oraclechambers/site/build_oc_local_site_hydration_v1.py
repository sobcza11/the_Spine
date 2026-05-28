from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import pandas as pd


RUN_TS = datetime.now(timezone.utc).isoformat()

INPUT_JSON = Path(
    "data/serving/oraclechambers/oc_local_deployment_manifest_v1.json"
)

OUTPUT_JSON = Path(
    "data/serving/oraclechambers/oc_local_site_hydration_v1.json"
)

OUTPUT_PARQUET = Path(
    "data/serving/oraclechambers/oc_local_site_hydration_v1.parquet"
)


def load_manifest() -> Dict[str, Any]:
    if not INPUT_JSON.exists():
        raise FileNotFoundError(
            f"Missing required input: {INPUT_JSON}"
        )

    with INPUT_JSON.open(
        "r",
        encoding="utf-8",
    ) as f:
        return json.load(f)


def load_asset(path: str) -> Dict[str, Any]:
    asset_path = Path(path)

    if not asset_path.exists():
        raise FileNotFoundError(
            f"Missing asset file: {asset_path}"
        )

    with asset_path.open(
        "r",
        encoding="utf-8",
    ) as f:
        return json.load(f)


def build_site_payload(
    manifest: Dict[str, Any]
) -> Dict[str, Any]:

    registry = manifest["asset_registry"]

    geoscen = load_asset(
        registry["geoscen_synthesis"]["path"]
    )

    dashboard = load_asset(
        registry["oc_executive_dashboard"]["path"]
    )

    runtime = load_asset(
        registry["oc_runtime_controller"]["path"]
    )

    layout = load_asset(
        registry["oc_adaptive_layout"]["path"]
    )

    return {
        "site_title": "OracleChambers | GeoScen",
        "site_mode": "offline_first",
        "runtime_mode": runtime[
            "runtime_controls"
        ]["runtime_mode"],
        "deployment_ready": manifest[
            "deployment_ready"
        ],
        "headline": {
            "regime": geoscen["regime"][
                "regime_label"
            ],
            "confidence": geoscen["metrics"][
                "confidence"
            ],
            "conviction": geoscen["metrics"][
                "conviction"
            ],
            "macro_temperature": geoscen[
                "metrics"
            ]["macro_temperature"],
            "risk_posture": geoscen[
                "synthesis"
            ]["risk_posture"],
            "decision_bias": geoscen[
                "synthesis"
            ]["decision_bias"],
        },
        "dashboard": {
            "header": dashboard[
                "dashboard_header"
            ],
            "layout_regions": dashboard[
                "layout_regions"
            ],
            "overlay_regions": dashboard[
                "overlay_regions"
            ],
            "chart_regions": dashboard[
                "chart_regions"
            ],
        },
        "frontend": {
            "layout_mode": layout[
                "frontend_state"
            ]["layout_mode"],
            "focus_panel": layout[
                "frontend_state"
            ]["focus_panel"],
            "overlay_priority": layout[
                "overlay_priority"
            ],
            "chart_layout": layout[
                "chart_layout"
            ],
        },
        "narrative": {
            "executive_summary": geoscen[
                "synthesis"
            ]["executive_summary"],
            "rbl_summary": geoscen[
                "synthesis"
            ]["rbl_summary"],
        },
        "historical_memory": geoscen[
            "historical_memory"
        ],
    }


def build_hydration_payload(
    manifest: Dict[str, Any]
) -> Dict[str, Any]:

    site_payload = build_site_payload(
        manifest
    )

    deployment_ready = bool(
        manifest.get(
            "deployment_ready",
            False,
        )
        and site_payload[
            "deployment_ready"
        ]
    )

    return {
        "artifact": "oc_local_site_hydration_v1",
        "system": "OracleChambers",
        "layer": "OC Local Site Hydration",
        "version": "v1",
        "run_ts": RUN_TS,
        "deployment_ready": deployment_ready,
        "site_payload": site_payload,
        "routing": {
            "local_site_ready": True,
            "offline_first": True,
            "online_runtime_ready": False,
            "frontend_hydration_ready": True,
            "executive_dashboard_ready": True,
            "ai_dependency": False,
        },
        "provenance": {
            "source_payload": str(INPUT_JSON),
            "source_artifact": manifest[
                "artifact"
            ],
            "source_run_ts": manifest[
                "run_ts"
            ],
        },
    }


def write_outputs(
    payload: Dict[str, Any]
) -> None:

    OUTPUT_JSON.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with OUTPUT_JSON.open(
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(
            payload,
            f,
            indent=2,
            ensure_ascii=False,
        )

    site = payload["site_payload"]
    headline = site["headline"]

    parquet_row = {
        "artifact": payload["artifact"],
        "system": payload["system"],
        "layer": payload["layer"],
        "version": payload["version"],
        "run_ts": payload["run_ts"],
        "deployment_ready": payload[
            "deployment_ready"
        ],
        "site_title": site["site_title"],
        "site_mode": site["site_mode"],
        "runtime_mode": site["runtime_mode"],
        "regime": headline["regime"],
        "confidence": headline["confidence"],
        "conviction": headline["conviction"],
        "macro_temperature": headline[
            "macro_temperature"
        ],
        "risk_posture": headline[
            "risk_posture"
        ],
        "decision_bias": headline[
            "decision_bias"
        ],
        "ai_dependency": payload[
            "routing"
        ]["ai_dependency"],
    }

    pd.DataFrame([parquet_row]).to_parquet(
        OUTPUT_PARQUET,
        index=False,
    )


def main() -> None:

    manifest = load_manifest()

    hydration_payload = build_hydration_payload(
        manifest
    )

    write_outputs(
        hydration_payload
    )

    site = hydration_payload[
        "site_payload"
    ]

    headline = site[
        "headline"
    ]

    print(
        "OC Local Site Hydration v1 complete"
    )

    print(
        f"deployment_ready: {hydration_payload['deployment_ready']}"
    )

    print(
        f"site_mode: {site['site_mode']}"
    )

    print(
        f"runtime_mode: {site['runtime_mode']}"
    )

    print(
        f"regime: {headline['regime']}"
    )

    print(f"json_output: {OUTPUT_JSON}")

    print(f"parquet_output: {OUTPUT_PARQUET}")


if __name__ == "__main__":
    main()

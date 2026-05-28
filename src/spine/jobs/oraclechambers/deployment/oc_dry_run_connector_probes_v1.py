from datetime import datetime, timezone
from typing import Any


CONNECTORS = ["Tiingo", "FRED", "Treasury", "Polygon", "EIA", "WRDS", "NBIS"]


def build_dry_run_connector_probes_v1() -> dict[str, Any]:
    probes = [
        {
            "connector": c,
            "dry_run_only": True,
            "credential_required": True,
            "live_call_executed": False,
            "runtime_mutation_allowed": False,
            "status": "defined_not_executed",
        }
        for c in CONNECTORS
    ]

    return {
        "artifact": "oc_dry_run_connector_probes_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "connector_probe_ready": True,
        "online_transition_allowed": False,
        "live_data_ingestion_enabled": False,
        "probes": probes,
    }


if __name__ == "__main__":
    print(build_dry_run_connector_probes_v1())

    
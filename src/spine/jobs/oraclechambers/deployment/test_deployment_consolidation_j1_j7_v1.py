from datetime import datetime, timezone
from typing import Any

from oc_master_deployment_status_dashboard_v1 import build_master_deployment_status_dashboard_v1
from oc_final_offline_rc_manifest_v1 import build_final_offline_rc_manifest_v1
from oc_hybrid_staging_preflight_validator_v1 import build_hybrid_staging_preflight_validator_v1
from oc_dry_run_connector_probes_v1 import build_dry_run_connector_probes_v1
from oc_operator_approval_record_v1 import build_operator_approval_record_v1
from oc_offline_rc_package_bundle_manifest_v1 import build_offline_rc_package_bundle_manifest_v1


def main() -> None:
    dashboard = build_master_deployment_status_dashboard_v1()
    rc = build_final_offline_rc_manifest_v1()
    preflight = build_hybrid_staging_preflight_validator_v1()
    probes = build_dry_run_connector_probes_v1()
    approval = build_operator_approval_record_v1()
    package = build_offline_rc_package_bundle_manifest_v1()

    layers = [dashboard, rc, preflight, probes, approval, package]
    failures: list[str] = []

    for layer in layers:
        if layer.get("online_transition_allowed"):
            failures.append(f"{layer['artifact']}:online_gate_open")

    if not rc.get("offline_certified"):
        failures.append("offline_rc_not_certified")

    if not package.get("package_ready"):
        failures.append("offline_package_not_ready")

    if probes.get("live_data_ingestion_enabled"):
        failures.append("live_ingestion_should_be_disabled")

    if approval.get("approval_granted"):
        failures.append("approval_should_not_be_granted_by_default")

    result: dict[str, Any] = {
        "artifact": "test_deployment_consolidation_j1_j7_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "passed": len(failures) == 0,
        "failed_checks": failures,
        "validated_layers": [layer["artifact"] for layer in layers],
        "hybrid_preflight_ready": preflight["hybrid_preflight_ready"],
        "hybrid_blocking_items": preflight["blocking_items"],
    }

    print(result)

    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

    
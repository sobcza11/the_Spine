from datetime import datetime, timezone
from typing import Any

from oc_credential_vault_integration_v1 import build_credential_vault_integration_v1
from oc_live_connector_authentication_v1 import build_live_connector_authentication_v1
from oc_real_staging_environment_v1 import build_real_staging_environment_v1
from oc_online_synchronization_testing_v1 import build_online_synchronization_testing_v1
from oc_runtime_degradation_testing_v1 import build_runtime_degradation_testing_v1
from oc_long_duration_uptime_testing_v1 import build_long_duration_uptime_testing_v1
from oc_operator_workflow_validation_v1 import build_operator_workflow_validation_v1
from oc_containerization_contract_v1 import build_containerization_contract_v1
from oc_ci_cd_release_pipeline_contract_v1 import build_ci_cd_release_pipeline_contract_v1
from oc_staging_deployment_infrastructure_v1 import build_staging_deployment_infrastructure_v1


def main() -> None:
    layers = [
        build_credential_vault_integration_v1(),
        build_live_connector_authentication_v1(),
        build_real_staging_environment_v1(),
        build_online_synchronization_testing_v1(),
        build_runtime_degradation_testing_v1(),
        build_long_duration_uptime_testing_v1(),
        build_operator_workflow_validation_v1(),
        build_containerization_contract_v1(),
        build_ci_cd_release_pipeline_contract_v1(),
        build_staging_deployment_infrastructure_v1(),
    ]

    failures: list[str] = []

    for layer in layers:
        if layer.get("online_transition_allowed"):
            failures.append(f"{layer['artifact']}:online_gate_open")

    if layers[0].get("all_credentials_loaded"):
        failures.append("credentials_should_not_be_assumed_loaded")

    if layers[1].get("live_auth_passed"):
        failures.append("live_auth_should_not_pass_without_credentials")

    if layers[3].get("sync_passed"):
        failures.append("sync_should_not_pass_without_online_mirror")

    if layers[5].get("uptime_certified"):
        failures.append("uptime_should_not_be_certified_yet")

    if layers[7].get("container_build_executed"):
        failures.append("container_build_should_not_execute")

    if layers[8].get("pipeline_executed"):
        failures.append("pipeline_should_not_execute")

    if layers[9].get("deployment_executed"):
        failures.append("staging_deployment_should_not_execute")

    result: dict[str, Any] = {
        "artifact": "test_staging_ops_layers_1_10_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "passed": len(failures) == 0,
        "failed_checks": failures,
        "validated_layers": [layer["artifact"] for layer in layers],
        "staging_ops_ready_for_manual_next_step": len(failures) == 0,
    }

    print(result)

    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

    
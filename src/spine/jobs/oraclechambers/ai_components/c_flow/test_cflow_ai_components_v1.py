from datetime import datetime, timezone
from typing import Any

from oc_cflow_zt_zeitgeist_v1 import build_cflow_zt_zeitgeist_v1
from oc_cflow_rbl_interpretation_v1 import build_cflow_rbl_interpretation_v1


def main() -> None:
    zt = build_cflow_zt_zeitgeist_v1()
    rbl = build_cflow_rbl_interpretation_v1()

    failures: list[str] = []

    if not zt.get("ai_component_ready"):
        failures.append("cflow_zt_not_ready")

    if not rbl.get("ai_component_ready"):
        failures.append("cflow_rbl_not_ready")

    if zt.get("online_transition_allowed"):
        failures.append("cflow_zt_online_gate_open")

    if rbl.get("online_transition_allowed"):
        failures.append("cflow_rbl_online_gate_open")

    if rbl.get("source_artifact") != zt.get("artifact"):
        failures.append("rbl_source_artifact_mismatch")

    if zt.get("plane") != "C_FLOW":
        failures.append("zt_plane_invalid")

    if rbl.get("plane") != "C_FLOW":
        failures.append("rbl_plane_invalid")

    result: dict[str, Any] = {
        "artifact": "test_cflow_ai_components_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "passed": len(failures) == 0,
        "failed_checks": failures,
        "validated_components": [
            zt["artifact"],
            rbl["artifact"],
        ],
    }

    print(result)

    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

    
from datetime import datetime, timezone
from typing import Any

from oc_fx_zt_zeitgeist_v1 import build_fx_zt_zeitgeist_v1
from oc_fx_rbl_interpretation_v1 import build_fx_rbl_interpretation_v1


def main() -> None:
    zt = build_fx_zt_zeitgeist_v1()
    rbl = build_fx_rbl_interpretation_v1()

    failures: list[str] = []

    if not zt.get("ai_component_ready"):
        failures.append("fx_zt_not_ready")

    if not rbl.get("ai_component_ready"):
        failures.append("fx_rbl_not_ready")

    if zt.get("online_transition_allowed"):
        failures.append("fx_zt_online_gate_open")

    if rbl.get("online_transition_allowed"):
        failures.append("fx_rbl_online_gate_open")

    if rbl.get("source_artifact") != zt.get("artifact"):
        failures.append("rbl_source_artifact_mismatch")

    if zt.get("plane") != "FX":
        failures.append("zt_plane_invalid")

    if rbl.get("plane") != "FX":
        failures.append("rbl_plane_invalid")

    result: dict[str, Any] = {
        "artifact": "test_fx_ai_components_v1",
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

    
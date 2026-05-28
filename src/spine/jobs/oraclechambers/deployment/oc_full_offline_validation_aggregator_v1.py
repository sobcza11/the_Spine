from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import importlib.util
import json


REPO_ROOT = Path(__file__).resolve().parents[5]

SEGMENTS_DIR = (
    REPO_ROOT
    / "src"
    / "spine"
    / "jobs"
    / "oraclechambers"
    / "segments"
)

RUNTIME_DIR = (
    REPO_ROOT
    / "src"
    / "spine"
    / "jobs"
    / "oraclechambers"
    / "runtime"
)

SECURITY_DIR = (
    REPO_ROOT
    / "src"
    / "spine"
    / "jobs"
    / "oraclechambers"
    / "security"
)

ONLINE_DIR = (
    REPO_ROOT
    / "src"
    / "spine"
    / "jobs"
    / "oraclechambers"
    / "online"
)

DEPLOYMENT_DIR = (
    REPO_ROOT
    / "src"
    / "spine"
    / "jobs"
    / "oraclechambers"
    / "deployment"
)

TEST_MODULES = [
    (
        "phase_a_core_cognition_sites",
        SEGMENTS_DIR / "test_phase_a_offline_cognition_sites_v1.py",
    ),
    (
        "phase_b_domain_planes",
        SEGMENTS_DIR / "test_phase_b_domain_plane_aggregator_v1.py",
    ),
    (
        "phase_c_nlp_planes",
        SEGMENTS_DIR / "test_phase_c_full_nlp_stack_v1.py",
    ),
    (
        "phase_d_visual_planes_1_3",
        SEGMENTS_DIR / "test_phase_d_visual_planes_1_3_v1.py",
    ),
    (
        "phase_d_visual_planes_4_6",
        SEGMENTS_DIR / "test_phase_d_visual_planes_4_6_v1.py",
    ),
    (
        "phase_e_runtime_layers_1_3",
        RUNTIME_DIR / "test_phase_e_runtime_layers_1_3_v1.py",
    ),
    (
        "phase_e_runtime_layers_4_6",
        RUNTIME_DIR / "test_phase_e_runtime_layers_4_6_v1.py",
    ),
    (
        "phase_f_security_layers_1_5",
        SECURITY_DIR / "test_phase_f_security_layers_1_5_v1.py",
    ),
    (
        "phase_f_security_layers_6_10",
        SECURITY_DIR / "test_phase_f_security_layers_6_10_v1.py",
    ),
    (
        "phase_g_online_transition_layers_1_3",
        ONLINE_DIR / "test_phase_g_online_transition_layers_1_3_v1.py",
    ),
    (
        "phase_g_online_transition_layers_4_5",
        ONLINE_DIR / "test_phase_g_online_transition_layers_4_5_v1.py",
    ),
]


def run_test_module(name: str, path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "name": name,
            "path": str(path),
            "passed": False,
            "error": "missing_test_module",
        }

    spec = importlib.util.spec_from_file_location(
        f"oc_test_{name}",
        path,
    )

    if spec is None or spec.loader is None:
        return {
            "name": name,
            "path": str(path),
            "passed": False,
            "error": "could_not_load_spec",
        }

    module = importlib.util.module_from_spec(spec)

    try:
        spec.loader.exec_module(module)

        if hasattr(module, "main"):
            module.main()

        return {
            "name": name,
            "path": str(path),
            "passed": True,
            "error": None,
        }

    except SystemExit as exc:
        return {
            "name": name,
            "path": str(path),
            "passed": exc.code in (0, None),
            "error": f"system_exit:{exc.code}",
        }

    except Exception as exc:
        return {
            "name": name,
            "path": str(path),
            "passed": False,
            "error": repr(exc),
        }


def build_full_offline_validation_aggregator_v1() -> dict[str, Any]:
    results = [
        run_test_module(name, path)
        for name, path in TEST_MODULES
    ]

    failed = [
        result for result in results
        if not result["passed"]
    ]

    return {
        "artifact": "oc_full_offline_validation_aggregator_v1",
        "layer": "OracleChambers Full Offline Validation Aggregator",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "offline_validation_passed": len(failed) == 0,
        "online_transition_allowed": False,
        "total_tests": len(results),
        "failed_tests": len(failed),
        "results": results,
    }


if __name__ == "__main__":
    print(
        json.dumps(
            build_full_offline_validation_aggregator_v1(),
            indent=2,
        )
    )
    
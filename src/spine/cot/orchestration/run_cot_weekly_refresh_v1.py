from datetime import datetime, UTC
from pathlib import Path
import json
import subprocess
import sys


PIPELINE_STEPS = [
    "spine.cot.live.build_cot_weekly_update_engine_v1",
    "spine.cot.panels.build_cot_instrument_panels_v1",

    # Conditioning layer
    "spine.cot.conditioning.build_cot_conditioned_panel_v1",

    # COT signal stack
    "spine.cot.signals.build_cot_normalized_positioning_v1",
    "spine.cot.signals.build_cot_crowding_percentiles_v1",
    "spine.cot.signals.build_cot_positioning_acceleration_v1",
    "spine.cot.signals.build_cot_unwind_probability_v1",

    # COT routing
    "spine.cot.routing.build_cot_geoscen_route_v1",
    "spine.cot.routing.build_cot_iv_vector_map_v1",
    "spine.cot.routing.build_cot_cross_asset_contagion_v1",
    "spine.cot.routing.build_cot_regime_conditioned_behavior_v1",

    # GeoScen conditioning
    "spine.geoscen.conditioning.build_geoscen_signal_conditioning_registry_v1",
    "spine.geoscen.conditioning.build_geoscen_conditioned_cot_routing_v1",

    # Validation
    "spine.cot.validation.build_cot_historical_stress_validation_v1",
]


def run_step(module_name: str) -> dict:
    started_at = datetime.now(UTC)

    result = subprocess.run(
        [sys.executable, "-m", module_name],
        capture_output=True,
        text=True,
    )

    finished_at = datetime.now(UTC)

    return {
        "module": module_name,
        "returncode": result.returncode,
        "started_at_utc": started_at.isoformat(),
        "finished_at_utc": finished_at.isoformat(),
        "stdout": result.stdout[-5000:],
        "stderr": result.stderr[-5000:],
        "status": "success" if result.returncode == 0 else "failed",
    }


def run_cot_weekly_refresh_v1():
    repo_root = Path.cwd()

    out_dir = repo_root / "data" / "cot" / "orchestration"
    out_dir.mkdir(parents=True, exist_ok=True)

    run_started = datetime.now(UTC)

    results = []

    for module_name in PIPELINE_STEPS:
        print("=" * 80)
        print(f"Running: {module_name}")
        print("=" * 80)

        step_result = run_step(module_name)
        results.append(step_result)

        print(step_result["stdout"])

        if step_result["stderr"]:
            print(step_result["stderr"])

        if step_result["returncode"] != 0:
            print(f"FAILED: {module_name}")
            break

    run_finished = datetime.now(UTC)

    status = (
        "success"
        if all(step["returncode"] == 0 for step in results)
        and len(results) == len(PIPELINE_STEPS)
        else "failed"
    )

    summary = {
        "component": "cot_weekly_refresh_v1",
        "status": status,
        "run_started_utc": run_started.isoformat(),
        "run_finished_utc": run_finished.isoformat(),
        "steps_total": len(PIPELINE_STEPS),
        "steps_completed": sum(1 for step in results if step["returncode"] == 0),
        "steps_failed": sum(1 for step in results if step["returncode"] != 0),
        "failed_modules": [
            step["module"]
            for step in results
            if step["returncode"] != 0
        ],
        "conditioning_enabled": True,
        "geoscen_quality_reweighting_enabled": True,
    }

    run_log = {
        "summary": summary,
        "steps": results,
    }

    summary_path = out_dir / "cot_weekly_refresh_summary_v1.json"
    log_path = out_dir / "cot_weekly_refresh_log_v1.json"

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(run_log, f, indent=2)

    print("=" * 80)
    print("COT WEEKLY REFRESH COMPLETE")
    print("=" * 80)
    print("Status:", status)
    print("Steps Completed:", summary["steps_completed"])
    print("Conditioning Enabled:", summary["conditioning_enabled"])
    print("GeoScen Reweighting Enabled:", summary["geoscen_quality_reweighting_enabled"])
    print("Summary:", summary_path)
    print("Log:", log_path)

    if status != "success":
        raise RuntimeError(f"COT weekly refresh failed: {summary['failed_modules']}")

    return summary


if __name__ == "__main__":
    run_cot_weekly_refresh_v1()

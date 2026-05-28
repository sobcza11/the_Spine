from pathlib import Path
from datetime import datetime, UTC
import json
import subprocess
import sys
import time
import pandas as pd


ORCHESTRATION_JOBS = [
    {
        "job_id": "registry_refresh",
        "priority": 1,
        "cadence": "daily",
        "module": "spine.registry.build_tier_status_registry_v1",
        "dependency": None,
        "domain": "control_plane",
    },
    {
        "job_id": "tier_dashboard_refresh",
        "priority": 2,
        "cadence": "daily",
        "module": "spine.registry.build_tier_completion_dashboard_json_v1",
        "dependency": "registry_refresh",
        "domain": "control_plane",
    },
    {
        "job_id": "i2_refresh",
        "priority": 3,
        "cadence": "quarterly_or_on_new_filings",
        "module": "spine.i2.build_i2_corporate_fragility_propagation_v1",
        "dependency": None,
        "domain": "i2",
    },
    {
        "job_id": "recursive_state_memory_refresh",
        "priority": 4,
        "cadence": "daily",
        "module": "spine.propagation.build_recursive_state_memory_v1",
        "dependency": "registry_refresh",
        "domain": "propagation",
    },
    {
        "job_id": "cross_engine_bus_refresh",
        "priority": 5,
        "cadence": "daily",
        "module": "spine.propagation.build_cross_engine_propagation_bus_v1",
        "dependency": "recursive_state_memory_refresh",
        "domain": "propagation",
    },
    {
        "job_id": "escalation_sync_refresh",
        "priority": 6,
        "cadence": "daily",
        "module": "spine.propagation.build_escalation_synchronization_engine_v1",
        "dependency": "cross_engine_bus_refresh",
        "domain": "propagation",
    },
    {
        "job_id": "fusion_refresh",
        "priority": 7,
        "cadence": "daily",
        "module": "spine.fusion.build_cross_engine_fusion_score_v1",
        "dependency": "escalation_sync_refresh",
        "domain": "fusion",
    },
    {
        "job_id": "geoscen_synthesis_refresh",
        "priority": 8,
        "cadence": "daily",
        "module": "spine.geoscen.build_geoscen_executive_synthesis_integration_v1",
        "dependency": "fusion_refresh",
        "domain": "geoscen",
    },
    {
        "job_id": "executive_brief_refresh",
        "priority": 9,
        "cadence": "daily",
        "module": "spine.executive.build_executive_system_brief_v1",
        "dependency": "geoscen_synthesis_refresh",
        "domain": "executive",
    },
]


def run_module(module_name: str):
    start = time.time()
    result = subprocess.run(
        [sys.executable, "-m", module_name],
        capture_output=True,
        text=True,
    )
    elapsed = round(time.time() - start, 4)

    return {
        "returncode": result.returncode,
        "elapsed_seconds": elapsed,
        "stdout": result.stdout[-4000:],
        "stderr": result.stderr[-4000:],
    }


def build_live_data_orchestration_v1():
    root = Path.cwd()
    out = root / "data" / "orchestration"
    out.mkdir(parents=True, exist_ok=True)

    results = []
    completed = set()

    for job in sorted(ORCHESTRATION_JOBS, key=lambda x: x["priority"]):
        dependency = job.get("dependency")

        if dependency and dependency not in completed:
            status = "skipped_dependency_not_met"
            run_result = {
                "returncode": None,
                "elapsed_seconds": 0,
                "stdout": "",
                "stderr": f"Dependency not met: {dependency}",
            }
        else:
            run_result = run_module(job["module"])
            status = "complete" if run_result["returncode"] == 0 else "failed"

        if status == "complete":
            completed.add(job["job_id"])

        results.append({
            "job_id": job["job_id"],
            "priority": job["priority"],
            "cadence": job["cadence"],
            "module": job["module"],
            "dependency": dependency,
            "domain": job["domain"],
            "status": status,
            "returncode": run_result["returncode"],
            "elapsed_seconds": run_result["elapsed_seconds"],
            "stdout_tail": run_result["stdout"],
            "stderr_tail": run_result["stderr"],
            "executed_at_utc": datetime.now(UTC).isoformat(),
        })

    df = pd.DataFrame(results)

    summary = {
        "component": "live_data_orchestration_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "job_count": int(len(df)),
        "complete_count": int((df["status"] == "complete").sum()),
        "failed_count": int((df["status"] == "failed").sum()),
        "skipped_count": int(df["status"].str.startswith("skipped").sum()),
        "orchestration_status": "complete" if int((df["status"] == "failed").sum()) == 0 else "review_required",
        "status": "live_data_orchestration_complete",
    }

    df.to_parquet(out / "live_data_orchestration_run_v1.parquet", index=False)
    df.to_json(out / "live_data_orchestration_run_v1.json", orient="records", indent=2)

    with open(out / "live_data_orchestration_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    with open(out / "live_data_orchestration_run_v1.md", "w", encoding="utf-8") as f:
        f.write("# Live Data Orchestration Run\n\n")
        f.write(f"Generated: {summary['generated_at_utc']}\n\n")
        f.write("## Summary\n\n")
        for k, v in summary.items():
            if k not in ["component", "status"]:
                f.write(f"- {k}: {v}\n")

        f.write("\n## Jobs\n\n")
        f.write(df[[
            "priority",
            "job_id",
            "domain",
            "cadence",
            "status",
            "elapsed_seconds",
            "module",
        ]].to_markdown(index=False))

    print("Live Data Orchestration complete")
    print("Jobs:", summary["job_count"])
    print("Complete:", summary["complete_count"])
    print("Failed:", summary["failed_count"])
    print("Skipped:", summary["skipped_count"])
    print("Status:", summary["orchestration_status"])
    print("OUTPUT:", out / "live_data_orchestration_run_v1.md")


if __name__ == "__main__":
    build_live_data_orchestration_v1()

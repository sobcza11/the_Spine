from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


AUTOMATION_CHAIN = [
    "spine.orchestration.build_live_data_orchestration_v1",
    "spine.propagation.build_persistent_recursive_state_memory_v1",
    "spine.propagation.build_cross_engine_dynamic_conditioning_v1",
    "spine.fusion.build_cross_engine_fusion_score_v1",
    "spine.geoscen.build_geoscen_executive_synthesis_integration_v1",
    "spine.executive.build_executive_system_brief_v1",
]


def build_runtime_automation_manifest_v1():
    root = Path.cwd()
    out = root / "data" / "orchestration"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame([
        {
            "run_order": i + 1,
            "module": m,
            "automation_status": "registered",
            "cadence": "daily_or_on_data_refresh",
        }
        for i, m in enumerate(AUTOMATION_CHAIN)
    ])

    summary = {
        "component": "runtime_automation_manifest_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "automation_step_count": int(len(df)),
        "automation_status": "runtime_automation_registered",
        "status": "runtime_automation_manifest_complete",
    }

    df.to_parquet(out / "runtime_automation_manifest_v1.parquet", index=False)
    df.to_json(out / "runtime_automation_manifest_v1.json", orient="records", indent=2)

    with open(out / "runtime_automation_manifest_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Runtime Automation Manifest complete")
    print("Steps:", summary["automation_step_count"])
    print("Status:", summary["automation_status"])


if __name__ == "__main__":
    build_runtime_automation_manifest_v1()

from pathlib import Path
from datetime import datetime, UTC
import json


def read_json(path):
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


def build_executive_monitoring_layer_v1():
    root = Path.cwd()
    out = root / "data" / "executive"
    out.mkdir(parents=True, exist_ok=True)

    fusion = read_json(root / "data" / "fusion" / "cross_engine_fusion_score_summary_v1.json")
    geoscen = read_json(root / "data" / "geoscen" / "geoscen_executive_synthesis_integration_v1.json")
    memory = read_json(root / "data" / "propagation" / "persistent_recursive_state_memory_summary_v1.json")
    conditioning = read_json(root / "data" / "propagation" / "cross_engine_dynamic_conditioning_summary_v1.json")
    validation = read_json(root / "data" / "validation" / "historical_validation_summary_v1.json")
    automation = read_json(root / "data" / "orchestration" / "runtime_automation_manifest_summary_v1.json")

    monitor = {
        "component": "executive_monitoring_layer_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "fusion_pressure": fusion.get("fusion_pressure"),
        "fusion_state": fusion.get("fusion_state"),
        "geoscen_pressure": geoscen.get("geoscen_executive_state", {}).get("geoscen_pressure"),
        "geoscen_state": geoscen.get("geoscen_executive_state", {}).get("geoscen_state"),
        "recursive_memory_rows": memory.get("memory_rows"),
        "recursive_continuity_score": memory.get("continuity_score"),
        "dynamic_conditioning_pressure": conditioning.get("average_dynamic_conditioned_pressure"),
        "historical_validation_status": validation.get("validation_status"),
        "automation_status": automation.get("automation_status"),
        "monitoring_status": "executive_monitoring_active",
        "status": "executive_monitoring_layer_complete",
    }

    json_path = out / "executive_monitoring_layer_v1.json"
    json_path.write_text(json.dumps(monitor, indent=2), encoding="utf-8")

    md = []
    md.append("# Executive Monitoring Layer")
    md.append("")
    md.append(f"Generated: {monitor['generated_at_utc']}")
    md.append("")
    for k, v in monitor.items():
        if k not in ["component", "status"]:
            md.append(f"- {k}: {v}")

    md_path = out / "executive_monitoring_layer_v1.md"
    md_path.write_text("\n".join(md), encoding="utf-8")

    print("Executive Monitoring Layer complete")
    print("GeoScen state:", monitor["geoscen_state"])
    print("Fusion state:", monitor["fusion_state"])
    print("Monitoring:", monitor["monitoring_status"])
    print("OUTPUT:", md_path)


if __name__ == "__main__":
    build_executive_monitoring_layer_v1()

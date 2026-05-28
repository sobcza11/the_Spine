from pathlib import Path
from datetime import datetime, UTC
import json


def normalize_langroid_runtime_v1():

    root = Path.cwd()
    runtime = root / "_offline_site" / "langroid_runtime"
    src = runtime / "langroid_runtime_export.json"

    if not src.exists():
        raise FileNotFoundError(f"Missing Langroid runtime export: {src}")

    data = json.loads(src.read_text(encoding="utf-8"))

    normalized = {
        "component": "langroid_runtime_export",
        "version": "v1",
        "built_at_utc": datetime.now(UTC).isoformat(),
        "status": "normalized_runtime_available",
        "agent_count": data.get("agent_count"),
        "message_count": len(data.get("message_bus", [])),
        "escalation_path_count": len(data.get("escalation_graph", {})),
        "payload": data
    }

    out = runtime / "langroid_runtime_export_normalized.json"

    out.write_text(
        json.dumps(normalized, indent=2),
        encoding="utf-8"
    )

    summary = {
        "component": "langroid_runtime_normalization_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "agent_count": normalized["agent_count"],
        "message_count": normalized["message_count"],
        "escalation_path_count": normalized["escalation_path_count"],
        "status": "langroid_runtime_normalization_complete"
    }

    summary_path = root / "_offline_site" / "config" / "langroid_runtime_normalization_summary_v1.json"

    summary_path.write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8"
    )

    print("Langroid Runtime Normalization complete")
    print("Messages:", summary["message_count"])
    print("Escalation paths:", summary["escalation_path_count"])
    print("OUTPUT:", out)


if __name__ == "__main__":
    normalize_langroid_runtime_v1()

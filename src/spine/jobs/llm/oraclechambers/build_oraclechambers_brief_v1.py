import json
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path.cwd()

QUALAYER_PACKET = ROOT / "data" / "llm" / "qualayer" / "qualayer_packet_v1.json"
OUT_DIR = ROOT / "data" / "llm" / "oraclechambers"
EVENT_PATH = ROOT / "data" / "llm" / "event_monitor" / "llm_event_monitor_v1.jsonl"
LOG_PATH = ROOT / "data" / "llm" / "logs" / "llm_run_log_v1.jsonl"


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def write_jsonl(path: Path, record: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, default=str) + "\n")


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    packet = json.loads(QUALAYER_PACKET.read_text(encoding="utf-8"))

    lines = []
    lines.append("# OracleChambers Brief v1")
    lines.append("")
    lines.append(f"Created At: {utc_now()}")
    lines.append(f"Source Run ID: {packet['source_run_id']}")
    lines.append(f"Manifest Version: {packet['manifest_version']}")
    lines.append("")
    lines.append("## Operating Rules")
    lines.append("")
    lines.append("- This brief is read-only.")
    lines.append("- It does not create signals.")
    lines.append("- It does not forecast.")
    lines.append("- It uses approved R2 inputs only.")
    lines.append("")
    lines.append("## Approved Inputs")
    lines.append("")

    for name, meta in packet["approved_signal_summary"].items():
        lines.append(f"### {name}")
        lines.append(f"- Source Key: `{meta['source_key']}`")
        lines.append(f"- Rows: {meta['rows']}")
        lines.append(f"- Latest Date: {meta['latest_date']}")
        lines.append(f"- Columns: {', '.join(meta['columns'])}")
        lines.append("")

    lines.append("## QuaLayer Instruction")
    lines.append("")
    lines.append("Use this packet as the controlled input layer for LLM-generated OracleChambers interpretation.")
    lines.append("All generated language must remain traceable to the approved inputs above.")
    lines.append("")

    out_path = OUT_DIR / "oraclechambers_brief_v1.md"
    out_path.write_text("\n".join(lines), encoding="utf-8")

    event = {
        "event_type": "oraclechambers_brief_built",
        "created_at": utc_now(),
        "source_run_id": packet["source_run_id"],
        "output_path": str(out_path),
    }

    write_jsonl(EVENT_PATH, event)
    write_jsonl(LOG_PATH, event)

    print("ORACLECHAMBERS_BRIEF_V1_BUILT")
    print(f"output={out_path}")


if __name__ == "__main__":
    main()
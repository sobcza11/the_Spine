import json
import re
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path.cwd()

QUALAYER_PACKET = ROOT / "data" / "llm" / "qualayer" / "qualayer_packet_v1.json"
ORACLE_BRIEF = ROOT / "data" / "llm" / "oraclechambers" / "oraclechambers_brief_v1.md"

EVENT_PATH = ROOT / "data" / "llm" / "event_monitor" / "llm_event_monitor_v1.jsonl"
LOG_PATH = ROOT / "data" / "llm" / "logs" / "llm_run_log_v1.jsonl"

REQUIRED_SECTIONS = [
    "# OracleChambers Brief v1",
    "## Operating Rules",
    "## Approved Inputs",
    "## QuaLayer Instruction",
]

FORBIDDEN_TERMS = [
    "predict",
    "prediction",
    "will rise",
    "will fall",
    "expected return",
    "price target",
    "buy signal",
    "sell signal",
    "recommendation",
]

REQUIRED_RULE_PHRASES = [
    "does not create signals",
    "does not forecast",
    "approved R2 inputs only",
    "traceable to the approved inputs",
]


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def write_jsonl(path: Path, record: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, default=str) + "\n")


def normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text.lower()).strip()


def main():
    failures = []

    if not QUALAYER_PACKET.exists():
        failures.append(f"Missing QuaLayer packet: {QUALAYER_PACKET}")

    if not ORACLE_BRIEF.exists():
        failures.append(f"Missing OracleChambers brief: {ORACLE_BRIEF}")

    if failures:
        raise SystemExit("; ".join(failures))

    packet = json.loads(QUALAYER_PACKET.read_text(encoding="utf-8"))
    brief_text = ORACLE_BRIEF.read_text(encoding="utf-8")
    brief_norm = normalize(brief_text)

    for section in REQUIRED_SECTIONS:
        if section not in brief_text:
            failures.append(f"Missing required section: {section}")

    for phrase in REQUIRED_RULE_PHRASES:
        if phrase.lower() not in brief_norm:
            failures.append(f"Missing required rule phrase: {phrase}")

    for term in FORBIDDEN_TERMS:
        if term in brief_norm:
            failures.append(f"Forbidden term found: {term}")

    approved_inputs = packet.get("approved_signal_summary", {})

    if not approved_inputs:
        failures.append("QuaLayer packet has no approved_signal_summary")

    for name, meta in approved_inputs.items():
        source_key = meta.get("source_key")

        if not source_key:
            failures.append(f"Missing source_key for approved input: {name}")
            continue

        if source_key not in brief_text:
            failures.append(f"OracleChambers brief does not reference source key: {source_key}")

    event = {
        "event_type": "oraclechambers_validation_v1",
        "created_at": utc_now(),
        "source_run_id": packet.get("source_run_id"),
        "brief_path": str(ORACLE_BRIEF),
        "qualayer_packet": str(QUALAYER_PACKET),
        "status": "failed" if failures else "passed",
        "failure_count": len(failures),
        "failures": failures,
    }

    write_jsonl(EVENT_PATH, event)
    write_jsonl(LOG_PATH, event)

    if failures:
        print("ORACLECHAMBERS_VALIDATION_FAILED")
        for failure in failures:
            print(f"FAIL: {failure}")
        raise SystemExit(1)

    print("ORACLECHAMBERS_VALIDATION_PASSED")
    print(f"brief={ORACLE_BRIEF}")
    print(f"source_run_id={packet.get('source_run_id')}")


if __name__ == "__main__":
    main()


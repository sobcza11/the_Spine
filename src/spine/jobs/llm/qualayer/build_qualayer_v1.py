import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


ROOT = Path.cwd()

INPUT_PACKET = ROOT / "data" / "llm" / "approved_inputs_packet_v1.json"
OUT_DIR = ROOT / "data" / "llm" / "qualayer"
EVENT_PATH = ROOT / "data" / "llm" / "event_monitor" / "llm_event_monitor_v1.jsonl"
LOG_PATH = ROOT / "data" / "llm" / "logs" / "llm_run_log_v1.jsonl"


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def write_jsonl(path: Path, record: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, default=str) + "\n")


def latest_date(df: pd.DataFrame):
    for col in ["date", "as_of_date", "period"]:
        if col in df.columns:
            return str(pd.to_datetime(df[col]).max().date())
    return None


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    packet = json.loads(INPUT_PACKET.read_text(encoding="utf-8"))
    inputs = packet["approved_inputs_loaded"]

    summaries = {}

    for name, meta in inputs.items():
        df = pd.read_parquet(meta["local_path"])
        summaries[name] = {
            "rows": int(len(df)),
            "columns": meta["columns"],
            "latest_date": latest_date(df),
            "source_key": meta["r2_key"],
            "sha256": meta["sha256"],
        }

    qualayer_packet = {
        "version": "v1",
        "layer": "QuaLayer",
        "created_at": utc_now(),
        "source_run_id": packet["run_id"],
        "manifest_version": packet["manifest_version"],
        "rules": {
            "llm_may_interpret": True,
            "llm_may_create_signals": False,
            "llm_may_forecast": False,
            "must_reference_source_fields": True,
            "approved_inputs_only": True,
        },
        "approved_signal_summary": summaries,
        "template_sections": {
            "macro_state": "Summarize current measured macro conditions only.",
            "policy_tone": "Describe central bank tone using approved signal fields only.",
            "pmi_zt": "Describe PMI/ISM qualitative pressure using approved Z_t input only.",
            "rates_alignment": "Describe rates alignment only where IsoVector rates join exists.",
            "limitations": "State missing data, lag, or unavailable fields explicitly.",
        },
    }

    out_path = OUT_DIR / "qualayer_packet_v1.json"
    out_path.write_text(json.dumps(qualayer_packet, indent=2, default=str), encoding="utf-8")

    event = {
        "event_type": "qualayer_packet_built",
        "created_at": utc_now(),
        "source_run_id": packet["run_id"],
        "output_path": str(out_path),
        "input_count": len(inputs),
    }

    write_jsonl(EVENT_PATH, event)
    write_jsonl(LOG_PATH, event)

    print("QUALAYER_PACKET_V1_BUILT")
    print(f"output={out_path}")


if __name__ == "__main__":
    main()


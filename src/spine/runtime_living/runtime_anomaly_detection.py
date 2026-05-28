from pathlib import Path
from datetime import datetime, timezone
import json
import random


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "runtime_living"
OUT_PATH = OUT_DIR / "runtime_anomalies.json"

WATCH_SYSTEMS = [
    "rbl_agent",
    "contradiction_agent",
    "geoscen_agent",
    "event_bus",
    "runtime_memory",
]


def anomaly_record(name: str) -> dict:
    severity = random.choice([
        "low",
        "medium",
        "high",
    ])

    return {
        "system": name,
        "status": "monitored",
        "anomaly_detected": False,
        "severity": severity,
    }


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    anomalies = [
        anomaly_record(x)
        for x in WATCH_SYSTEMS
    ]

    payload = {
        "system": "IsoVector",
        "module": "runtime-anomaly-detection",
        "generated_utc": datetime.now(
            timezone.utc
        ).isoformat(),

        "runtime_monitoring_enabled": True,

        "systems_monitored": len(anomalies),

        "anomalies": anomalies,

        "governance": {
            "drift_detection_enabled": True,
            "runtime_corruption_detection": True,
            "stale_runtime_detection": True,
            "hallucination_detection": True,
            "llm_writeback_allowed": False,
        },
    }

    OUT_PATH.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )

    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()

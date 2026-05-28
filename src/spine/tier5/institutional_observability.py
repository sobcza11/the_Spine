from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "tier5"
OUT_PATH = OUT_DIR / "institutional_observability.json"


METRICS = [
    {
        "metric": "runtime_latency_ms",
        "status": "monitored",
    },
    {
        "metric": "retrieval_quality_score",
        "status": "monitored",
    },
    {
        "metric": "anomaly_frequency",
        "status": "monitored",
    },
    {
        "metric": "websocket_health",
        "status": "monitored",
    },
    {
        "metric": "runtime_refresh_failures",
        "status": "monitored",
    },
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "institutional-observability",
        "generated_utc": datetime.now(
            timezone.utc
        ).isoformat(),

        "observability_enabled": True,

        "metric_count": len(METRICS),

        "metrics": METRICS,

        "governance": {
            "runtime_telemetry_enabled": True,
            "health_monitoring_required": True,
            "anomaly_visibility_required": True,
            "dashboard_monitoring_enabled": True,
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

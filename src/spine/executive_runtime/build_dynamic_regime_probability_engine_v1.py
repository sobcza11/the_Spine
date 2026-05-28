from pathlib import Path
from datetime import datetime, UTC
import json


def build_dynamic_regime_probability_engine_v1():

    root = Path.cwd()
    site = root / "_offline_site"

    pressure_path = (
        site
        / "executive_runtime"
        / "global_systemic_pressure_engine_v1.json"
    )

    if pressure_path.exists():
        pressure = json.loads(pressure_path.read_text(encoding="utf-8"))
        base = float(pressure.get("average_systemic_pressure", 0) or 0)
    else:
        base = 0.0

    probabilities = {
        "stable_monitoring": round(max(0, 1 - base), 4),
        "fragility_building": round(min(1, base * 1.25), 4),
        "recursive_deterioration": round(min(1, base * 0.85), 4),
        "systemic_escalation": round(min(1, base * 0.55), 4)
    }

    payload = {
        "component": "dynamic_regime_probability_engine_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "source_average_systemic_pressure": base,
        "probabilities": probabilities,
        "dominant_regime": max(probabilities, key=probabilities.get),
        "status": "dynamic_regime_probability_ready"
    }

    out = (
        site
        / "executive_runtime"
        / "dynamic_regime_probability_engine_v1.json"
    )

    out.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8"
    )

    print("Dynamic Regime Probability Engine complete")
    print("Dominant regime:", payload["dominant_regime"])
    print("OUTPUT:", out)


if __name__ == "__main__":
    build_dynamic_regime_probability_engine_v1()

from pathlib import Path
from datetime import datetime, UTC
import json


def build_global_systemic_pressure_engine_v1():

    root = Path.cwd()
    site = root / "_offline_site"

    candidate_files = [
        site / "geoscen_runtime" / "geoscen_rbl_synthesis_v1_normalized.json",
        site / "geoscen_runtime" / "geoscen_structural_macro_layer_v1_normalized.json",
        site / "geoscen_runtime" / "geoscen_contradiction_engine_v1_normalized.json",
        site / "finstate_payloads" / "finstate_executive_synthesis_v1.json",
        site / "langroid_runtime" / "langroid_runtime_export_normalized.json",
    ]

    components = []

    for path in candidate_files:

        if not path.exists():
            continue

        data = json.loads(path.read_text(encoding="utf-8"))
        payload = data.get("payload", data)

        score = (
            payload.get("temperature_score")
            or payload.get("structural_pressure_score")
            or payload.get("contradiction_score")
            or payload.get("fusion_pressure")
            or 0
        )

        components.append({
            "file": path.name,
            "component": data.get("component", payload.get("component", path.stem)),
            "score": float(score or 0),
            "status": data.get("status", payload.get("status", "available"))
        })

    avg_score = (
        sum(c["score"] for c in components) / len(components)
        if components
        else 0
    )

    payload = {
        "component": "global_systemic_pressure_engine_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": len(components),
        "average_systemic_pressure": round(avg_score, 4),
        "components": components,
        "status": "global_systemic_pressure_ready"
    }

    out = site / "executive_runtime" / "global_systemic_pressure_engine_v1.json"

    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Global Systemic Pressure Engine complete")
    print("Components:", payload["component_count"])
    print("Avg systemic pressure:", payload["average_systemic_pressure"])
    print("OUTPUT:", out)


if __name__ == "__main__":
    build_global_systemic_pressure_engine_v1()

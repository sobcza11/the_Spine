from pathlib import Path
from datetime import datetime, UTC
import json


def build_macro_corporate_commodity_fusion_v1():

    root = Path.cwd()
    site = root / "_offline_site"

    files = [
        site / "geoscen_runtime" / "geoscen_rbl_synthesis_v1_normalized.json",
        site / "finstate_payloads" / "finstate_executive_synthesis_v1.json",
        site / "geoscen_runtime" / "geoscen_structural_macro_layer_v1_normalized.json",
        site / "geoscen_runtime" / "geoscen_frontend_intelligence_layer_v1_normalized.json",
    ]

    components = []

    for f in files:

        if not f.exists():
            continue

        data = json.loads(f.read_text(encoding="utf-8"))
        payload = data.get("payload", data)

        components.append({
            "file": f.name,
            "component": data.get("component", payload.get("component", f.stem)),
            "status": data.get("status", payload.get("status", "available"))
        })

    fusion = {
        "component": "macro_corporate_commodity_fusion_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "fusion_domains": [
            "macro",
            "corporate_survivability",
            "commodity_flow"
        ],
        "component_count": len(components),
        "components": components,
        "fusion_state": "macro_corporate_commodity_fusion_ready",
        "status": "macro_corporate_commodity_fusion_ready"
    }

    out = site / "executive_runtime" / "macro_corporate_commodity_fusion_v1.json"

    out.write_text(json.dumps(fusion, indent=2), encoding="utf-8")

    print("Macro Corporate Commodity Fusion complete")
    print("Components:", fusion["component_count"])
    print("OUTPUT:", out)


if __name__ == "__main__":
    build_macro_corporate_commodity_fusion_v1()

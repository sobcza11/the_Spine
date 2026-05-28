from pathlib import Path
from datetime import datetime, UTC
import json


def build_operational_security_layer_v1():

    root = Path.cwd()
    site = root / "_offline_site"

    security = {
        "component": "operational_security_layer_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "security_controls": {
            "blocked_secret_patterns": [
                ".env",
                "secret",
                "token",
                "credential",
                "key.pem",
                "private"
            ],
            "static_auth_gate": True,
            "cloudflare_access_recommended": True,
            "no_local_machine_dependency": True,
            "static_payload_runtime": True
        },
        "status": "operational_security_layer_ready"
    }

    out = site / "cloudflare" / "manifests" / "operational_security_layer_v1.json"
    out.write_text(json.dumps(security, indent=2), encoding="utf-8")

    print("Operational Security Layer complete")
    print("Cloudflare Access recommended:", security["security_controls"]["cloudflare_access_recommended"])
    print("OUTPUT:", out)


if __name__ == "__main__":
    build_operational_security_layer_v1()

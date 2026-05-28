from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "tier5"
OUT_PATH = OUT_DIR / "api_gateway_governance.json"


ENDPOINTS = [
    {
        "endpoint": "/rbl/runtime",
        "auth_required": True,
        "rate_limit_per_min": 60,
    },
    {
        "endpoint": "/geoscen/runtime",
        "auth_required": True,
        "rate_limit_per_min": 30,
    },
    {
        "endpoint": "/dashboard/live",
        "auth_required": True,
        "rate_limit_per_min": 120,
    },
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "api-gateway-governance",
        "generated_utc": datetime.now(
            timezone.utc
        ).isoformat(),

        "gateway_governance_enabled": True,

        "endpoint_count": len(ENDPOINTS),

        "endpoints": ENDPOINTS,

        "governance": {
            "authenticated_ingress_required": True,
            "rate_limiting_enabled": True,
            "endpoint_audit_required": True,
            "runtime_boundary_enforced": True,
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

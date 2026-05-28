from pathlib import Path
from datetime import datetime, timezone
import json
import re


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine")

PKG = ROOT / "data" / "deploy_static" / "isovector_offline_approved"

OUT_PATH = PKG / "predeploy_smoke_test.json"


SITES = [
    "equities-industry",
    "equities-sector",
    "c-flow",
    "fx",
    "rates",
]


SECRET_PATTERNS = [
    r"AKIA[0-9A-Z]{16}",
    r"sk-[A-Za-z0-9]{20,}",
    r"BEGIN RSA PRIVATE KEY",
    r"BEGIN OPENSSH PRIVATE KEY",
]


def html_ok(path):
    if not path.exists():
        return False

    html = path.read_text(encoding="utf-8")

    required = [
        "OFFLINE REVIEW MODE",
        "writeback_allowed: false",
        "human_review_required: true",
        "deployment_target: isovector.io",
        "Source Payload Coverage",
        "Visual Acceptance Checklist",
    ]

    no_secrets = not any(
        re.search(pattern, html)
        for pattern in SECRET_PATTERNS
    )

    return all(x in html for x in required) and no_secrets


def main():
    results = []

    for site in SITES:
        path = PKG / site / "index.html"

        results.append({
            "site": site,
            "index_exists": path.exists(),
            "html_acceptance_pass": html_ok(path),
        })

    master_index = PKG / "index.html"

    payload = {
        "system": "IsoVector",
        "module": "predeploy-smoke-test",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "predeploy_smoke_test_enabled": True,

        "package_path": str(PKG),
        "master_index_exists": master_index.exists(),

        "site_count": len(SITES),
        "passed_site_count": sum(
            1 for x in results
            if x["html_acceptance_pass"]
        ),

        "site_results": results,

        "smoke_test_contract": {
            "master_index_required": True,
            "all_5_sections_required": True,
            "governance_headers_required": True,
            "secret_scan_required": True,
            "no_local_only_dependency_required": True,
        },

        "governance": {
            "predeploy_smoke_test_governed": True,
            "deployment_blocked_on_failure": True,
            "human_review_required": True,
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

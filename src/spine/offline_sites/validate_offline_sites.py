from pathlib import Path
from datetime import datetime, timezone
import json
import re


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine")

SITE_ROOT = ROOT / "data" / "offline_sites"

OUT_DIR = ROOT / "data" / "offline_sites"

OUT_PATH = OUT_DIR / "offline_site_validation.json"


SITES = [
    "equities-industry",
    "equities-sector",
    "c-flow",
    "fx",
    "rates",
]


REQUIRED_TEXT = [
    "OFFLINE REVIEW MODE",
    "writeback_allowed: false",
    "source_payloads_required: true",
    "human_review_required: true",
    "deployment_target: isovector.io",
    "Source Payload Coverage",
    "Visual Acceptance Checklist",
    "Z?",
    "RBL",
]


SECRET_PATTERNS = [
    r"AKIA[0-9A-Z]{16}",
    r"sk-[A-Za-z0-9]{20,}",
    r"BEGIN RSA PRIVATE KEY",
    r"BEGIN OPENSSH PRIVATE KEY",
]


def site_result(site):
    p = SITE_ROOT / site / "index.html"

    result = {
        "site": site,
        "index_exists": p.exists(),
        "required_text_pass": False,
        "secret_scan_pass": False,
        "status": "failed",
    }

    if not p.exists():
        return result

    html = p.read_text(encoding="utf-8")

    result["required_text_pass"] = all(
        x in html for x in REQUIRED_TEXT
    )

    result["secret_scan_pass"] = not any(
        re.search(pattern, html)
        for pattern in SECRET_PATTERNS
    )

    if result["required_text_pass"] and result["secret_scan_pass"]:
        result["status"] = "passed"

    return result


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    site_results = [
        site_result(site)
        for site in SITES
    ]

    master_index = SITE_ROOT / "index.html"

    payload = {
        "system": "IsoVector",
        "module": "offline-site-validator",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "offline_site_validator_enabled": True,

        "site_count": len(SITES),
        "passed_site_count": sum(
            1 for x in site_results
            if x["status"] == "passed"
        ),

        "master_index_exists": master_index.exists(),

        "site_results": site_results,

        "validation_contract": {
            "all_5_directories_required": True,
            "all_5_index_files_required": True,
            "governance_header_required": True,
            "source_payload_section_required": True,
            "secret_scan_required": True,
        },

        "governance": {
            "offline_validation_governed": True,
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

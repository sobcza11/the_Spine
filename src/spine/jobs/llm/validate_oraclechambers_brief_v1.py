from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path.cwd()

BRIEF_PATH = REPO_ROOT / "data" / "llm" / "oraclechambers" / "oraclechambers_brief_v1.md"
VALIDATION_PATH = REPO_ROOT / "data" / "llm" / "oraclechambers" / "oraclechambers_validation_v1.json"

REQUIRED_SECTIONS = [
    "# OracleChambers Brief v1",
    "## Validation Status",
    "## Source Boundary",
    "## Measured Macro State",
    "## PMI / ISM Pressure",
    "## Rates Alignment",
    "## Limitations",
]

FORBIDDEN_FORWARD_TERMS = [
    "will rise",
    "will fall",
    "will increase",
    "will decrease",
    "will tighten",
    "will ease",
    "expected to",
    "forecast is",
    "prediction",
    "projection",
    "target price",
]

REQUIRED_BOUNDARY_PHRASES = [
    "approved QuaLayer inputs",
    "No forecasting",
    "signal creation",
    "descriptive only",
]


def load_text(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"Required file not found: {path}")

    return path.read_text(encoding="utf-8")


def validate_brief(text: str) -> dict:
    errors: list[str] = []
    warnings: list[str] = []

    for section in REQUIRED_SECTIONS:
        if section not in text:
            errors.append(f"Missing required section: {section}")

    lower_text = text.lower()

    for term in FORBIDDEN_FORWARD_TERMS:
        if term in lower_text:
            errors.append(f"Forbidden forward-looking phrase found: {term}")

    for phrase in REQUIRED_BOUNDARY_PHRASES:
        if phrase.lower() not in lower_text:
            warnings.append(f"Boundary phrase not found: {phrase}")

    if "unavailable" in lower_text:

        if "rates alignment: unavailable" in lower_text or "curve spread: unavailable" in lower_text:
            warnings.append("Brief contains unavailable evidence fields")

    status = "PASS"
    if warnings:
        status = "WARN"
    if errors:
        status = "FAIL"

    return {
        "validation_version": "v1",
        "validated_at_utc": datetime.now(timezone.utc).isoformat(),
        "brief_path": str(BRIEF_PATH),
        "status": status,
        "errors": errors,
        "warnings": warnings,
    }


def main() -> None:
    text = load_text(BRIEF_PATH)
    result = validate_brief(text)

    VALIDATION_PATH.parent.mkdir(parents=True, exist_ok=True)

    with VALIDATION_PATH.open("w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)

    print(f"OracleChambers validation written: {VALIDATION_PATH}")
    print(f"Validation status: {result['status']}")

    if result["errors"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()


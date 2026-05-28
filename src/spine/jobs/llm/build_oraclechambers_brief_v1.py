from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path.cwd()

PACKET_PATH = REPO_ROOT / "data" / "llm" / "qualayer" / "qualayer_packet_v1.json"
VALIDATION_PATH = REPO_ROOT / "data" / "llm" / "qualayer" / "qualayer_validation_v1.json"
OUTPUT_PATH = REPO_ROOT / "data" / "llm" / "oraclechambers" / "oraclechambers_brief_v1.md"


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Required file not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def fmt(value: Any) -> str:
    if value is None:
        return "unavailable"
    return str(value)


def build_brief(packet: dict[str, Any], validation: dict[str, Any]) -> str:
    if validation.get("status") == "FAIL":
        raise ValueError("Cannot build OracleChambers brief from failed QuaLayer validation")

    evidence = packet["evidence"]
    cb = evidence["central_bank"]
    pmi = evidence["pmi"]
    rates = evidence["rates"]

    warnings = validation.get("warnings", [])
    warning_text = "None" if not warnings else "; ".join(warnings)

    created_at = datetime.now(timezone.utc).isoformat()

    return f"""# OracleChambers Brief v1

Generated UTC: {created_at}

## Validation Status

Status: {validation.get("status")}
Warnings: {warning_text}

## Source Boundary

This brief uses only approved QuaLayer inputs.

No forecasting, signal creation, or narrative expansion beyond the approved packet is permitted.

## Measured Macro State

Central bank tone is {fmt(cb.get("policy_tone_direction"))} based on the approved IsoVector CB view.

Policy tone score: {fmt(cb.get("policy_tone_score"))}
Uncertainty score: {fmt(cb.get("uncertainty_score"))}
Bank: {fmt(cb.get("bank_code"))}
Date: {fmt(cb.get("date"))}

## PMI / ISM Pressure

PMI pressure is {fmt(pmi.get("pmi_pressure_direction"))} based on the approved GeoScen PMI input.

PMI pressure: {fmt(pmi.get("pmi_pressure"))}
Labor tag: {fmt(pmi.get("labor_tag"))}
Inflation tag: {fmt(pmi.get("inflation_tag"))}
Demand tag: {fmt(pmi.get("demand_tag"))}
Date: {fmt(pmi.get("date"))}

## Rates Alignment

Rates alignment evidence is {fmt(rates.get("rates_alignment_direction"))}.

Rates alignment: {fmt(rates.get("rates_alignment"))}
Curve spread: {fmt(rates.get("curve_spread"))}
Bank: {fmt(rates.get("bank_code"))}
Date: {fmt(rates.get("date"))}

## Limitations

Rates evidence is unavailable where fields are null.

This brief is descriptive only and does not forecast future conditions.
"""


def main() -> None:
    packet = load_json(PACKET_PATH)
    validation = load_json(VALIDATION_PATH)

    brief = build_brief(packet, validation)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT_PATH.open("w", encoding="utf-8") as f:
        f.write(brief)

    print(f"OracleChambers brief written: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
    
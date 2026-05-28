from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path.cwd()

PACKET_PATH = REPO_ROOT / "data" / "llm" / "qualayer" / "qualayer_packet_v1.json"
VALIDATION_PATH = REPO_ROOT / "data" / "llm" / "qualayer" / "qualayer_validation_v1.json"

VALID_DIRECTIONS = {"positive", "negative", "neutral", "unknown"}

FORBIDDEN_TERMS = {
    "forecast",
    "predict",
    "prediction",
    "expected",
    "expects",
    "will",
    "should",
    "likely",
    "outlook",
    "projection",
}


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Required file not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def has_forbidden_terms(value: Any) -> list[str]:
    hits: list[str] = []

    if isinstance(value, str):
        lower = value.lower()
        for term in FORBIDDEN_TERMS:
            if term in lower:
                hits.append(term)

    elif isinstance(value, dict):
        for nested in value.values():
            hits.extend(has_forbidden_terms(nested))

    elif isinstance(value, list):
        for nested in value:
            hits.extend(has_forbidden_terms(nested))

    return sorted(set(hits))


def validate_packet(packet: dict[str, Any]) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []

    required_top_level = [
        "packet_version",
        "system",
        "layer",
        "definition",
        "rules",
        "approved_inputs",
        "evidence",
        "rbl_scaffold",
    ]

    for key in required_top_level:
        if key not in packet:
            errors.append(f"Missing required top-level key: {key}")

    if packet.get("layer") != "QuaLayer":
        errors.append("Packet layer must equal QuaLayer")

    if packet.get("definition") != "structured_rbl_packet":
        errors.append("Packet definition must equal structured_rbl_packet")

    rules = packet.get("rules", {})
    required_rules = {
        "approved_inputs_only": True,
        "no_forecasting": True,
        "no_signal_creation": True,
        "no_narrative_generation": True,
        "qualayer_is_structured_rbl_only": True,
    }

    for key, expected in required_rules.items():
        if rules.get(key) is not expected:
            errors.append(f"Rule missing or incorrect: {key}")

    evidence = packet.get("evidence", {})

    if "central_bank" not in evidence:
        errors.append("Missing central_bank evidence")

    if "pmi" not in evidence:
        errors.append("Missing pmi evidence")

    if "rates" not in evidence:
        warnings.append("Missing rates evidence")
    else:
        rates = evidence["rates"]
        if rates.get("rates_alignment") is None:
            warnings.append("rates_alignment is null")
        if rates.get("curve_spread") is None:
            warnings.append("curve_spread is null")

    for section_name, section in evidence.items():
        if not isinstance(section, dict):
            errors.append(f"Evidence section is not an object: {section_name}")
            continue

        for key, value in section.items():
            if key.endswith("_direction") and value not in VALID_DIRECTIONS:
                errors.append(f"Invalid direction value: {section_name}.{key}={value}")

    scaffold = packet.get("rbl_scaffold", {})
    semantic_controls = scaffold.get("semantic_controls", {})
    required_controls = {
        "modality_required": True,
        "negation_required": True,
        "positive_wording_not_equal_positive_intent": True,
        "hknsl_mirror": True,
    }

    for key, expected in required_controls.items():
        if semantic_controls.get(key) is not expected:
            errors.append(f"Semantic control missing or incorrect: {key}")

    observations = scaffold.get("compressed_observations", [])
    for obs in observations:
        if isinstance(obs, str) and len(obs.split()) > 12:
            warnings.append(f"Compressed observation exceeds 12 words: {obs}")

    forbidden_hits = has_forbidden_terms(packet)
    if forbidden_hits:
        errors.append(f"Forbidden narrative/forecasting terms found: {forbidden_hits}")

    status = "PASS"
    if warnings:
        status = "WARN"
    if errors:
        status = "FAIL"

    return {
        "validation_version": "v1",
        "validated_at_utc": datetime.now(timezone.utc).isoformat(),
        "packet_path": str(PACKET_PATH),
        "packet_version": packet.get("packet_version"),
        "status": status,
        "errors": errors,
        "warnings": warnings,
    }


def main() -> None:
    packet = load_json(PACKET_PATH)
    result = validate_packet(packet)

    VALIDATION_PATH.parent.mkdir(parents=True, exist_ok=True)

    with VALIDATION_PATH.open("w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)

    print(f"QuaLayer validation written: {VALIDATION_PATH}")
    print(f"Validation status: {result['status']}")

    if result["errors"]:
        raise SystemExit(1)




if __name__ == "__main__":
    main()


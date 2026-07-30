"""Read-only governance gate for index-profile lens evaluation eligibility."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd

from spine.equities.index_pipeline import CANONICAL_COLUMNS, PRICE_COLUMNS, VOLUME_COLUMNS
from spine.equities.index_profiles import REPO_ROOT

OBJECT_TYPE = "QQQ_LENS_ELIGIBILITY_V1"
LENSES = ("MARKET_BREADTH", "VOLATILITY_STRUCTURE", "LIQUIDITY_FLOWS")
EXPECTED_IDENTITY = {
    "instrument_id": "QQQ",
    "provider": "TIINGO",
    "provider_dataset": "DAILY_EOD_PRICES",
}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("LENS_METADATA_OBJECT_REQUIRED")
    return value


def _blocked_result(reason_codes: set[str], evidence: dict[str, Any]) -> dict[str, Any]:
    lenses = {
        "MARKET_BREADTH": {
            "status": "BLOCKED",
            "reason_codes": [
                "CROSS_SECTIONAL_BREADTH_INPUTS_MISSING",
                "HISTORY_REQUIREMENT_UNPROVEN",
            ],
        },
        "VOLATILITY_STRUCTURE": {
            "status": "BLOCKED",
            "reason_codes": [
                "LENS_INPUT_CONTRACT_UNPROVEN",
                "HISTORY_REQUIREMENT_UNPROVEN",
            ],
        },
        "LIQUIDITY_FLOWS": {
            "status": "BLOCKED",
            "reason_codes": [
                "LENS_INPUT_CONTRACT_UNPROVEN",
                "HISTORY_REQUIREMENT_UNPROVEN",
            ],
        },
    }
    for lens in lenses.values():
        reason_codes.update(lens["reason_codes"])
    return {
        "object_type": OBJECT_TYPE,
        "schema_version": "1.0.0",
        "instrument_id": evidence.get("instrument_id", "QQQ"),
        "artifact_hash": evidence.get("artifact_hash"),
        "metadata_hash": evidence.get("metadata_hash"),
        "observation_range": {
            "start": evidence.get("observation_start"),
            "end": evidence.get("observation_end"),
        },
        "record_count": evidence.get("record_count"),
        "eligible_lenses": [],
        "blocked_lenses": list(LENSES),
        "lens_assessments": lenses,
        "reason_codes": sorted(reason_codes),
        "runtime_state": evidence.get("runtime_state", "VALIDATION_ONLY"),
        "publication_state": evidence.get("publication_state", "UNKNOWN"),
        "lifecycle": evidence.get("lifecycle", "UNKNOWN"),
        "activation_performed": False,
    }


def evaluate_qqq_lens_eligibility(
    *,
    serving_path: Path,
    metadata_path: Path,
    canonical_path: Path,
    profile_path: Path,
) -> dict[str, Any]:
    reasons: set[str] = set()
    evidence: dict[str, Any] = {"instrument_id": "QQQ"}

    if not serving_path.exists():
        reasons.add("SERVING_ARTIFACT_MISSING")
    if not metadata_path.exists():
        reasons.add("SERVING_METADATA_MISSING")
    if not canonical_path.exists():
        reasons.add("CANONICAL_ARTIFACT_MISSING")
    if not profile_path.exists():
        reasons.add("INDEX_PROFILE_MISSING")
    if reasons:
        return _blocked_result(reasons, evidence)

    try:
        metadata = _load_json(metadata_path)
        profile = _load_json(profile_path)
        frame = pd.read_parquet(serving_path)
    except (OSError, ValueError, json.JSONDecodeError):
        reasons.add("REQUIRED_EVIDENCE_INVALID")
        return _blocked_result(reasons, evidence)

    evidence.update({
        "artifact_hash": _sha256(serving_path),
        "metadata_hash": _sha256(metadata_path),
        "record_count": len(frame),
        "runtime_state": metadata.get("runtime_state", "UNKNOWN"),
        "publication_state": metadata.get("serving_publication", "UNKNOWN"),
        "lifecycle": metadata.get("lifecycle", "UNKNOWN"),
    })

    for field, expected in EXPECTED_IDENTITY.items():
        if metadata.get(field) != expected:
            reasons.add(f"{field.upper()}_MISMATCH")
    if profile.get("asset_class") != "EQUITY":
        reasons.add("ASSET_CLASS_MISMATCH")
    if profile.get("instrument_type") != "ETF_INDEX_PROXY":
        reasons.add("INSTRUMENT_TYPE_MISMATCH")
    if profile.get("instrument_id") != "QQQ":
        reasons.add("PROFILE_INSTRUMENT_MISMATCH")

    if evidence["artifact_hash"] != metadata.get("serving_artifact_sha256"):
        reasons.add("SERVING_ARTIFACT_HASH_MISMATCH")
    if _sha256(canonical_path) != metadata.get("source_canonical_artifact_sha256"):
        reasons.add("CANONICAL_ARTIFACT_HASH_MISMATCH")
    if metadata.get("serving_publication") != "COMPLETE":
        reasons.add("SERVING_PUBLICATION_INCOMPLETE")
    if metadata.get("runtime_state") != "VALIDATION_ONLY":
        reasons.add("RUNTIME_STATE_INVALID")
    if metadata.get("lifecycle") != "SHADOW":
        reasons.add("LIFECYCLE_INVALID")
    if metadata.get("production_publication") != "PROHIBITED":
        reasons.add("PRODUCTION_BOUNDARY_INVALID")
    if metadata.get("oraclechambers_eligibility") != "NOT_AUTHORIZED":
        reasons.add("ORACLECHAMBERS_BOUNDARY_INVALID")

    if list(frame.columns) != list(CANONICAL_COLUMNS):
        reasons.add("SERVING_SCHEMA_MISMATCH")
    required = set(CANONICAL_COLUMNS)
    if not required.issubset(frame.columns):
        reasons.add("REQUIRED_FIELDS_MISSING")
    else:
        symbols = set(frame["symbol"].astype(str).str.upper())
        if symbols != {"QQQ"}:
            reasons.add("SYMBOL_IDENTITY_INVALID")
        if "SPY" in symbols:
            reasons.add("SPY_CONTAMINATION")
        if len(symbols) > 1:
            reasons.add("CROSS_INDEX_ROWS")
        dates = pd.to_datetime(frame["date"], errors="coerce")
        if dates.isna().any():
            reasons.add("OBSERVATION_DATE_INVALID")
        else:
            evidence["observation_start"] = dates.min().date().isoformat()
            evidence["observation_end"] = dates.max().date().isoformat()
            if not dates.is_monotonic_increasing:
                reasons.add("OBSERVATIONS_NOT_ASCENDING")
        if frame.duplicated(["symbol", "date"]).any():
            reasons.add("DUPLICATE_OBSERVATION_KEY")
        numeric = frame[list(PRICE_COLUMNS) + list(VOLUME_COLUMNS) + ["split_factor"]]
        if numeric.isna().any().any():
            reasons.add("REQUIRED_NUMERIC_NULL")
        if (frame[list(PRICE_COLUMNS)] <= 0).any().any():
            reasons.add("INVALID_PRICE")
        if (frame[list(VOLUME_COLUMNS)] < 0).any().any():
            reasons.add("NEGATIVE_VOLUME")
        if (frame["split_factor"] <= 0).any():
            reasons.add("INVALID_SPLIT_FACTOR")
        adjusted = {"adj_open", "adj_high", "adj_low", "adj_close", "adj_volume"}
        raw = {"open", "high", "low", "close", "volume"}
        if not adjusted.issubset(frame.columns) or not raw.issubset(frame.columns):
            reasons.add("RAW_ADJUSTED_SEPARATION_INVALID")

    if metadata.get("observation_count") != len(frame):
        reasons.add("RECORD_COUNT_MISMATCH")
    if evidence.get("observation_start") and metadata.get("observation_start", "")[:10] != evidence["observation_start"]:
        reasons.add("OBSERVATION_START_MISMATCH")
    if evidence.get("observation_end") and metadata.get("observation_end", "")[:10] != evidence["observation_end"]:
        reasons.add("OBSERVATION_END_MISMATCH")

    # No governed lens-specific minimum-history or missing-session policy exists.
    reasons.add("HISTORY_REQUIREMENT_UNPROVEN")
    reasons.add("MISSING_OBSERVATION_POLICY_UNPROVEN")
    return _blocked_result(reasons, evidence)


def check_qqq_lens_eligibility(repo_root: Path = REPO_ROOT) -> dict[str, Any]:
    return evaluate_qqq_lens_eligibility(
        serving_path=repo_root / "data/serving/equities/indexes/qqq_daily_eod_v1.parquet",
        metadata_path=repo_root / "data/serving/equities/indexes/qqq_daily_eod_v1.metadata.json",
        canonical_path=repo_root / "data/canonical/equities/indexes/qqq_daily_eod_v1.parquet",
        profile_path=repo_root / "config/equities/index_profiles/qqq.json",
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.parse_args()
    print(json.dumps(check_qqq_lens_eligibility(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

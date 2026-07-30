"""Governed, network-free canonical observation publication for an index profile."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from spine.equities.index_pipeline import CANONICAL_COLUMNS, canonicalize_tiingo_daily
from spine.equities.index_profiles import REPO_ROOT, load_authorization, load_profile
from spine.equities.provenance.canonical_observation_metadata import (
    build_canonical_observation_metadata,
    compute_canonical_observation_metadata_id,
    validate_canonical_observation_metadata,
)

METHOD_ID = "EQUITIES_COMMON_INDEX_TIINGO_CANONICALIZATION_V1"
METHOD_VERSION = "1.0.0"
EXPECTED_RAW_FIELDS = {
    "date", "open", "high", "low", "close", "volume",
    "adjOpen", "adjHigh", "adjLow", "adjClose", "adjVolume",
    "divCash", "splitFactor",
}
RESIDUE_SUFFIXES = (".candidate", ".bak", ".rollback")


def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("QQQ_INPUT_MANIFEST_OBJECT_REQUIRED")
    return value


def _load_rows(path: Path) -> list[dict[str, Any]]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, list) or not all(isinstance(row, dict) for row in value):
        raise ValueError("QQQ_INPUT_RAW_ROWS_REQUIRED")
    return value


def verify_inputs(
    instrument_id: str,
    *,
    repo_root: Path = REPO_ROOT,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], Path, Path, list[dict[str, Any]]]:
    profile = load_profile(instrument_id)
    authorization = load_authorization(profile["acquisition_policy"]["authorization_reference"], repo_root)
    raw_path = repo_root / authorization["raw_destination"]
    manifest_path = repo_root / authorization["acquisition_manifest_destination"]
    manifest = _load_object(manifest_path)
    rows = _load_rows(raw_path)
    instrument = instrument_id.upper()
    expected = {
        "provider": profile["provider"],
        "provider_dataset": profile["provider_dataset"],
        "record_count": len(rows),
        "source_observation_start": authorization["approved_start"],
        "source_observation_end": authorization["approved_end"],
        "symbols": [instrument],
    }
    mismatches = [key for key, value in expected.items() if manifest.get(key) != value]
    evidence = manifest.get("provider_evidence", {})
    parameters = manifest.get("request_parameters", {})
    if evidence.get("authorization_id") != authorization["authorization_id"]:
        mismatches.append("authorization_id")
    if parameters != {
        "symbol": instrument,
        "startDate": authorization["approved_start"],
        "endDate": authorization["approved_end"],
    }:
        mismatches.append("request_parameters")
    if profile["instrument_id"] != instrument or profile["provider_symbol"] != instrument:
        mismatches.append("instrument_identity")
    if _hash(raw_path) != manifest.get("raw_artifact_sha256"):
        mismatches.append("raw_artifact_sha256")
    if manifest.get("record_count") != 275:
        mismatches.append("record_count")
    if evidence.get("request_count") != 1 or evidence.get("retry_count") != 0:
        mismatches.append("request_policy")
    if mismatches:
        raise ValueError(f"QQQ_INPUT_EVIDENCE_MISMATCH:{','.join(sorted(set(mismatches)))}")
    if not rows:
        raise ValueError("QQQ_INPUT_EMPTY")
    missing = sorted(EXPECTED_RAW_FIELDS - set.intersection(*(set(row) for row in rows)))
    if missing:
        raise ValueError(f"QQQ_INPUT_RAW_FIELD_MISSING:{missing[0]}")
    dates = pd.to_datetime([row["date"] for row in rows], errors="raise", utc=True)
    if dates.duplicated().any():
        raise ValueError("QQQ_INPUT_DUPLICATE_DATE")
    if not dates.is_monotonic_increasing:
        raise ValueError("QQQ_INPUT_DATE_ORDER_INVALID")
    return profile, authorization, manifest, raw_path, manifest_path, rows


def build_canonical_candidate(
    instrument_id: str,
    rows: list[dict[str, Any]],
    authorization: dict[str, Any],
) -> pd.DataFrame:
    missing = sorted(
        field for field in EXPECTED_RAW_FIELDS
        if any(field not in row for row in rows)
    )
    if missing:
        raise ValueError(f"INDEX_CANONICALIZATION_FIELD_MISSING:{missing[0]}")
    frame = canonicalize_tiingo_daily(pd.DataFrame(rows), instrument_id)
    start = pd.Timestamp(authorization["approved_start"])
    end = pd.Timestamp(authorization["approved_end"])
    if ((frame["date"] < start) | (frame["date"] > end)).any():
        raise ValueError("INDEX_CANONICALIZATION_DATE_OUTSIDE_AUTHORIZATION")
    if set(frame["symbol"]) != {instrument_id.upper()}:
        raise ValueError("INDEX_CANONICALIZATION_INSTRUMENT_INVALID")
    return frame


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def _build_metadata(
    canonical_path: Path,
    profile: dict[str, Any],
    authorization: dict[str, Any],
    manifest: dict[str, Any],
    raw_path: Path,
    frame: pd.DataFrame,
) -> dict[str, Any]:
    metadata = build_canonical_observation_metadata(
        dataset_id=f"{profile['instrument_id']}_DAILY_EOD_PRICES",
        canonical_artifact=canonical_path,
        canonicalization_method_id=METHOD_ID,
        canonicalization_method_version=METHOD_VERSION,
        manifests=[manifest],
        provider=profile["provider"],
        universe=profile["universe_designation"],
        symbols=[profile["instrument_id"]],
        observation_start=frame["date"].min().strftime("%Y-%m-%dT00:00:00Z"),
        observation_end=frame["date"].max().strftime("%Y-%m-%dT00:00:00Z"),
        record_count=len(frame),
        field_contract={
            "columns": list(CANONICAL_COLUMNS),
            "immutable_identifiers": ["symbol", "date"],
            "observation_key": ["symbol", "date"],
            "raw_adjusted_separation": "REQUIRED",
        },
        transformation_contract={
            "sort_order": ["symbol", "date"],
            "duplicate_policy": "REJECT",
            "deterministic_generation": True,
        },
        corporate_action_contract=profile["corporate_action_policy"],
        lifecycle=profile["lifecycle"],
    )
    metadata.update({
        "instrument_id": profile["instrument_id"],
        "provider_dataset": profile["provider_dataset"],
        "source_acquisition_manifest_id": manifest["manifest_id"],
        "source_raw_artifact": str(raw_path),
        "source_raw_artifact_sha256": manifest["raw_artifact_sha256"],
        "authorization_id": authorization["authorization_id"],
        "observation_key": ["symbol", "date"],
        "canonical_schema_version": profile["canonical_schema_version"],
        "raw_adjusted_separation_policy": profile["corporate_action_policy"]["policy_id"],
        "validation_status": "VALIDATED",
        "publication_status": "CANONICAL_ONLY",
        "serving_status": "NOT_PUBLISHED",
        "lens_eligibility_status": "PENDING_NO_ACTIVATION",
        "production_status": "NOT_ACTIVATED",
    })
    metadata["metadata_id"] = compute_canonical_observation_metadata_id(metadata)
    return validate_canonical_observation_metadata(metadata, canonical_path, [manifest])


def validate_pair(
    canonical_path: Path,
    metadata_path: Path,
    *,
    expected_count: int,
    expected_instrument: str,
    expected_start: str,
    expected_end: str,
    expected_raw_hash: str,
) -> dict[str, Any]:
    frame = pd.read_parquet(canonical_path)
    metadata = _load_object(metadata_path)
    if list(frame.columns) != list(CANONICAL_COLUMNS):
        raise ValueError("QQQ_CANONICAL_COLUMN_ORDER_INVALID")
    if len(frame) != expected_count or frame.duplicated(["symbol", "date"]).any():
        raise ValueError("QQQ_CANONICAL_COUNT_OR_KEY_INVALID")
    if set(frame["symbol"]) != {expected_instrument}:
        raise ValueError("QQQ_CANONICAL_INSTRUMENT_INVALID")
    dates = pd.to_datetime(frame["date"])
    if not frame.equals(frame.sort_values(["symbol", "date"], kind="mergesort").reset_index(drop=True)):
        raise ValueError("QQQ_CANONICAL_SORT_INVALID")
    if dates.min().date().isoformat() != expected_start or dates.max().date().isoformat() != expected_end:
        raise ValueError("QQQ_CANONICAL_RANGE_INVALID")
    if metadata.get("source_raw_artifact_sha256") != expected_raw_hash:
        raise ValueError("QQQ_CANONICAL_RAW_HASH_INVALID")
    validate_canonical_observation_metadata(metadata, canonical_path)
    return metadata


def _adjacent(path: Path, suffix: str) -> Path:
    return path.with_name(f".{path.name}{suffix}")


def _replace(source: Path, destination: Path, phase: str) -> None:
    os.replace(source, destination)


def publish_pair(
    frame: pd.DataFrame,
    profile: dict[str, Any],
    authorization: dict[str, Any],
    manifest: dict[str, Any],
    raw_path: Path,
    *,
    repo_root: Path = REPO_ROOT,
    validator: Callable[..., dict[str, Any]] = validate_pair,
) -> tuple[Path, Path, dict[str, Any]]:
    canonical_path = repo_root / profile["serving_contract"]["canonical_path"]
    metadata_path = repo_root / profile["serving_contract"]["canonical_metadata_path"]
    if canonical_path == repo_root / "data/canonical/equities/indexes/spy_daily_eod_v1.parquet":
        raise ValueError("QQQ_CANONICAL_SPY_PATH_COLLISION")
    canonical_path.parent.mkdir(parents=True, exist_ok=True)
    candidates = [_adjacent(canonical_path, ".candidate"), _adjacent(metadata_path, ".candidate")]
    backups = [_adjacent(canonical_path, ".bak"), _adjacent(metadata_path, ".bak")]
    for residue in [*candidates, *backups]:
        if residue.exists():
            raise ValueError(f"QQQ_CANONICAL_PREEXISTING_RESIDUE:{residue.name}")
    kwargs = {
        "expected_count": len(frame),
        "expected_instrument": profile["instrument_id"],
        "expected_start": authorization["approved_start"],
        "expected_end": authorization["approved_end"],
        "expected_raw_hash": manifest["raw_artifact_sha256"],
    }
    existed = [canonical_path.exists(), metadata_path.exists()]
    if any(existed) and not all(existed):
        raise ValueError("QQQ_CANONICAL_PREEXISTING_PAIR_INCONSISTENT")
    if all(existed):
        validator(canonical_path, metadata_path, **kwargs)
    try:
        frame.to_parquet(candidates[0], index=False)
        metadata = _build_metadata(candidates[0], profile, authorization, manifest, raw_path, frame)
        metadata["canonical_artifact"] = str(canonical_path)
        metadata["evidence"] = [{"artifact": str(canonical_path)}]
        metadata["metadata_id"] = compute_canonical_observation_metadata_id(metadata)
        _write_json(candidates[1], metadata)
        validator(candidates[0], candidates[1], **kwargs)
        if all(existed):
            shutil.copy2(canonical_path, backups[0])
            shutil.copy2(metadata_path, backups[1])
        _replace(candidates[0], canonical_path, "publish_canonical")
        _replace(candidates[1], metadata_path, "publish_metadata")
        validator(canonical_path, metadata_path, **kwargs)
    except Exception:
        for index, destination in enumerate((canonical_path, metadata_path)):
            if backups[index].exists():
                rollback = _adjacent(destination, ".rollback")
                shutil.copy2(backups[index], rollback)
                _replace(rollback, destination, f"rollback_{index}")
            elif not existed[index] and destination.exists():
                destination.unlink()
        raise
    finally:
        for residue in [*candidates, *backups, _adjacent(canonical_path, ".rollback"), _adjacent(metadata_path, ".rollback")]:
            if residue.exists():
                residue.unlink()
    return canonical_path, metadata_path, _load_object(metadata_path)


def canonicalize_authorized_index(instrument_id: str, *, repo_root: Path = REPO_ROOT) -> dict[str, Any]:
    profile, authorization, manifest, raw_path, _, rows = verify_inputs(instrument_id, repo_root=repo_root)
    frame = build_canonical_candidate(instrument_id, rows, authorization)
    canonical_path, metadata_path, metadata = publish_pair(
        frame, profile, authorization, manifest, raw_path, repo_root=repo_root
    )
    return {
        "classification": "QQQ_CANONICAL_OBSERVATION_COMPLETE",
        "canonical_artifact": str(canonical_path),
        "canonical_metadata": str(metadata_path),
        "canonical_artifact_sha256": metadata["canonical_artifact_sha256"],
        "record_count": len(frame),
        "observation_start": metadata["observation_start"],
        "observation_end": metadata["observation_end"],
        "network_execution": False,
        "serving_publication": False,
        "lens_activation": False,
        "production_activation": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--instrument", required=True)
    args = parser.parse_args()
    print(json.dumps(canonicalize_authorized_index(args.instrument), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

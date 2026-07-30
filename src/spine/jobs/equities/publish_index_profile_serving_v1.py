"""Publish a governed index profile's canonical observations to SHADOW serving."""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from spine.equities.index_pipeline import (
    CANONICAL_COLUMNS,
    PRICE_COLUMNS,
    VOLUME_COLUMNS,
)
from spine.equities.index_profiles import (
    REPO_ROOT,
    load_all_profiles,
    load_authorization,
    load_profile,
)
from spine.equities.provenance.canonical_observation_metadata import (
    load_canonical_observation_metadata,
    validate_canonical_observation_metadata,
)
from spine.equities.provenance.serving_export_metadata import (
    build_serving_export_metadata,
    compute_serving_export_metadata_id,
    validate_serving_export_metadata,
)
from spine.jobs.equities.canonicalize_index_profile_v1 import (
    _adjacent,
    _hash,
    _load_object,
    _replace,
    _write_json,
)

EXPORTER_ID = "EQUITIES_PROFILE_PARQUET_SERVING_EXPORT_V1"
EXPORTER_VERSION = "1.0.0"
FIXED_QQQ_CANONICAL_HASH = (
    "544c5ee3327c64c313c293daa56fbf1569920faeafb4174bff30228fa237be51"
)


def verify_canonical_input(
    instrument_id: str,
    *,
    repo_root: Path = REPO_ROOT,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], Path, Path, pd.DataFrame]:
    profile = load_profile(instrument_id)
    authorization = load_authorization(
        profile["acquisition_policy"]["authorization_reference"], repo_root
    )
    canonical_path = repo_root / profile["serving_contract"]["canonical_path"]
    metadata_path = repo_root / profile["serving_contract"]["canonical_metadata_path"]
    if not canonical_path.exists():
        raise ValueError("INDEX_SERVING_CANONICAL_MISSING")
    if not metadata_path.exists():
        raise ValueError("INDEX_SERVING_CANONICAL_METADATA_MISSING")
    metadata = load_canonical_observation_metadata(metadata_path)
    validate_canonical_observation_metadata(metadata, canonical_path)
    frame = pd.read_parquet(canonical_path)
    expected = {
        "instrument_id": profile["instrument_id"],
        "provider": profile["provider"],
        "provider_dataset": profile["provider_dataset"],
        "authorization_id": authorization["authorization_id"],
        "record_count": 275,
        "observation_start": f"{authorization['approved_start']}T00:00:00Z",
        "observation_end": f"{authorization['approved_end']}T00:00:00Z",
        "lifecycle": "SHADOW",
        "publication_status": "CANONICAL_ONLY",
        "serving_status": "NOT_PUBLISHED",
    }
    mismatches = [field for field, value in expected.items() if metadata.get(field) != value]
    canonical_hash = _hash(canonical_path)
    if canonical_hash != metadata.get("canonical_artifact_sha256"):
        mismatches.append("canonical_artifact_sha256")
    if instrument_id.upper() == "QQQ" and canonical_hash != FIXED_QQQ_CANONICAL_HASH:
        mismatches.append("fixed_qqq_canonical_hash")
    if list(frame.columns) != list(CANONICAL_COLUMNS):
        mismatches.append("canonical_columns")
    if len(frame) != 275 or frame.duplicated(["symbol", "date"]).any():
        mismatches.append("canonical_observation_key")
    if set(frame["symbol"]) != {instrument_id.upper()}:
        mismatches.append("canonical_instrument")
    dates = pd.to_datetime(frame["date"])
    if dates.min().date().isoformat() != authorization["approved_start"]:
        mismatches.append("canonical_start")
    if dates.max().date().isoformat() != authorization["approved_end"]:
        mismatches.append("canonical_end")
    if not frame.equals(frame.sort_values(["symbol", "date"], kind="mergesort").reset_index(drop=True)):
        mismatches.append("canonical_sort")
    if (frame[list(PRICE_COLUMNS)] <= 0).any().any():
        mismatches.append("canonical_price")
    if (frame[list(VOLUME_COLUMNS)] < 0).any().any():
        mismatches.append("canonical_volume")
    if (frame["split_factor"] <= 0).any():
        mismatches.append("canonical_split_factor")
    if mismatches:
        raise ValueError(f"INDEX_SERVING_CANONICAL_INVALID:{','.join(sorted(set(mismatches)))}")
    return profile, authorization, metadata, canonical_path, metadata_path, frame


def _validate_destinations(profile: dict[str, Any], repo_root: Path) -> tuple[Path, Path]:
    serving_path = repo_root / profile["serving_contract"]["serving_path"]
    metadata_path = repo_root / profile["serving_contract"]["serving_metadata_path"]
    all_profiles = load_all_profiles()
    for other in all_profiles:
        if other["instrument_id"] == profile["instrument_id"]:
            continue
        other_paths = other["serving_contract"]
        if profile["serving_contract"]["serving_path"] in {
            other_paths["serving_path"],
            other_paths["serving_metadata_path"],
        }:
            raise ValueError("INDEX_SERVING_PROFILE_PATH_COLLISION")
    forbidden = {
        repo_root / "data/serving/equities/us_equity_index_data.json",
        repo_root / "data/serving/equities/us_equity_index_t1.parquet",
    }
    if serving_path in forbidden or "qqq" not in serving_path.name.lower():
        raise ValueError("INDEX_SERVING_AGGREGATE_OR_INSTRUMENT_PATH_COLLISION")
    if serving_path == metadata_path:
        raise ValueError("INDEX_SERVING_PAIR_PATH_COLLISION")
    return serving_path, metadata_path


def build_serving_candidate(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.loc[:, list(CANONICAL_COLUMNS)].copy()
    return result.sort_values(["symbol", "date"], kind="mergesort").reset_index(drop=True)


def _build_metadata(
    serving_candidate: Path,
    canonical_path: Path,
    canonical_metadata_path: Path,
    canonical_metadata: dict[str, Any],
    profile: dict[str, Any],
    authorization: dict[str, Any],
) -> dict[str, Any]:
    metadata = build_serving_export_metadata(
        serving_artifact=serving_candidate,
        exporter="spine.jobs.equities.publish_index_profile_serving_v1",
        exporter_method_id=EXPORTER_ID,
        exporter_method_version=EXPORTER_VERSION,
        canonical_artifact=canonical_path,
        canonical_metadata=canonical_metadata,
        acquisition_manifest_ids=canonical_metadata["acquisition_manifests"],
        provider=profile["provider"],
        universe=profile["universe_designation"],
        symbols=[profile["instrument_id"]],
        observation_start=canonical_metadata["observation_start"],
        observation_end=canonical_metadata["observation_end"],
        lifecycle="SHADOW",
    )
    metadata.update({
        "instrument_id": profile["instrument_id"],
        "provider_dataset": profile["provider_dataset"],
        "authorization_id": authorization["authorization_id"],
        "source_canonical_artifact": str(canonical_path),
        "source_canonical_artifact_sha256": canonical_metadata["canonical_artifact_sha256"],
        "source_canonical_metadata": str(canonical_metadata_path),
        "observation_count": canonical_metadata["record_count"],
        "observation_key": ["symbol", "date"],
        "serving_schema_version": "1.0.0",
        "retained_fields": list(CANONICAL_COLUMNS),
        "omitted_fields": [],
        "corporate_action_policy": profile["corporate_action_policy"]["policy_id"],
        "raw_adjusted_separation_policy": profile["corporate_action_policy"]["policy_id"],
        "runtime_state": "VALIDATION_ONLY",
        "validation_status": "VALIDATED",
        "serving_publication": "COMPLETE",
        "production_publication": "PROHIBITED",
        "lens_eligibility": "PENDING",
        "oraclechambers_eligibility": "NOT_AUTHORIZED",
    })
    metadata["metadata_id"] = compute_serving_export_metadata_id(metadata)
    return validate_serving_export_metadata(metadata, serving_candidate, canonical_metadata)


def validate_serving_pair(
    serving_path: Path,
    metadata_path: Path,
    *,
    canonical_path: Path,
    canonical_metadata: dict[str, Any],
    profile: dict[str, Any],
    authorization: dict[str, Any],
) -> dict[str, Any]:
    frame = pd.read_parquet(serving_path)
    metadata = _load_object(metadata_path)
    validate_serving_export_metadata(metadata, serving_path, canonical_metadata)
    mismatches: list[str] = []
    if list(frame.columns) != list(CANONICAL_COLUMNS):
        mismatches.append("columns")
    if metadata.get("retained_fields") != list(CANONICAL_COLUMNS) or metadata.get("omitted_fields") != []:
        mismatches.append("field_declaration")
    if len(frame) != 275 or metadata.get("observation_count") != 275:
        mismatches.append("record_count")
    if frame.duplicated(["symbol", "date"]).any() or set(frame["symbol"]) != {profile["instrument_id"]}:
        mismatches.append("identity")
    if not frame.equals(frame.sort_values(["symbol", "date"], kind="mergesort").reset_index(drop=True)):
        mismatches.append("sort")
    if metadata.get("source_canonical_artifact_sha256") != _hash(canonical_path):
        mismatches.append("canonical_hash")
    required = {
        "provider": profile["provider"],
        "provider_dataset": profile["provider_dataset"],
        "authorization_id": authorization["authorization_id"],
        "lifecycle": "SHADOW",
        "runtime_state": "VALIDATION_ONLY",
        "serving_publication": "COMPLETE",
        "production_publication": "PROHIBITED",
        "lens_eligibility": "PENDING",
        "oraclechambers_eligibility": "NOT_AUTHORIZED",
    }
    mismatches.extend(field for field, value in required.items() if metadata.get(field) != value)
    dates = pd.to_datetime(frame["date"])
    if dates.min().date().isoformat() != authorization["approved_start"]:
        mismatches.append("start")
    if dates.max().date().isoformat() != authorization["approved_end"]:
        mismatches.append("end")
    if (frame[list(PRICE_COLUMNS)] <= 0).any().any():
        mismatches.append("price")
    if (frame[list(VOLUME_COLUMNS)] < 0).any().any() or (frame["split_factor"] <= 0).any():
        mismatches.append("corporate_action")
    if mismatches:
        raise ValueError(f"INDEX_SERVING_PAIR_INVALID:{','.join(sorted(set(mismatches)))}")
    return metadata


def publish_pair(
    frame: pd.DataFrame,
    profile: dict[str, Any],
    authorization: dict[str, Any],
    canonical_path: Path,
    canonical_metadata_path: Path,
    canonical_metadata: dict[str, Any],
    *,
    repo_root: Path = REPO_ROOT,
    validator: Callable[..., dict[str, Any]] = validate_serving_pair,
) -> tuple[Path, Path, dict[str, Any]]:
    serving_path, metadata_path = _validate_destinations(profile, repo_root)
    serving_path.parent.mkdir(parents=True, exist_ok=True)
    candidates = [_adjacent(serving_path, ".candidate"), _adjacent(metadata_path, ".candidate")]
    backups = [_adjacent(serving_path, ".bak"), _adjacent(metadata_path, ".bak")]
    rollbacks = [_adjacent(serving_path, ".rollback"), _adjacent(metadata_path, ".rollback")]
    for residue in [*candidates, *backups, *rollbacks]:
        if residue.exists():
            raise ValueError(f"INDEX_SERVING_PREEXISTING_RESIDUE:{residue.name}")
    existed = [serving_path.exists(), metadata_path.exists()]
    if any(existed) and not all(existed):
        raise ValueError("INDEX_SERVING_PREEXISTING_PAIR_INCONSISTENT")
    kwargs = {
        "canonical_path": canonical_path,
        "canonical_metadata": canonical_metadata,
        "profile": profile,
        "authorization": authorization,
    }
    if all(existed):
        validator(serving_path, metadata_path, **kwargs)
    try:
        frame.to_parquet(candidates[0], index=False)
        metadata = _build_metadata(
            candidates[0], canonical_path, canonical_metadata_path,
            canonical_metadata, profile, authorization,
        )
        metadata["serving_artifact"] = str(serving_path)
        metadata["evidence"] = [{"artifact": str(serving_path)}]
        metadata["metadata_id"] = compute_serving_export_metadata_id(metadata)
        _write_json(candidates[1], metadata)
        validator(candidates[0], candidates[1], **kwargs)
        if all(existed):
            shutil.copy2(serving_path, backups[0])
            shutil.copy2(metadata_path, backups[1])
        _replace(candidates[0], serving_path, "publish_serving")
        _replace(candidates[1], metadata_path, "publish_metadata")
        validator(serving_path, metadata_path, **kwargs)
    except Exception:
        for index, destination in enumerate((serving_path, metadata_path)):
            if backups[index].exists():
                shutil.copy2(backups[index], rollbacks[index])
                _replace(rollbacks[index], destination, f"rollback_{index}")
            elif not existed[index] and destination.exists():
                destination.unlink()
        raise
    finally:
        for residue in [*candidates, *backups, *rollbacks]:
            if residue.exists():
                residue.unlink()
    return serving_path, metadata_path, _load_object(metadata_path)


def publish_index_profile(instrument_id: str, *, repo_root: Path = REPO_ROOT) -> dict[str, Any]:
    profile, authorization, canonical_metadata, canonical_path, metadata_path, frame = (
        verify_canonical_input(instrument_id, repo_root=repo_root)
    )
    serving_frame = build_serving_candidate(frame)
    serving_path, serving_metadata_path, serving_metadata = publish_pair(
        serving_frame, profile, authorization, canonical_path, metadata_path,
        canonical_metadata, repo_root=repo_root,
    )
    return {
        "classification": "QQQ_GOVERNED_SERVING_PUBLICATION_COMPLETE",
        "serving_artifact": str(serving_path),
        "serving_metadata": str(serving_metadata_path),
        "serving_artifact_sha256": serving_metadata["serving_artifact_sha256"],
        "record_count": len(serving_frame),
        "observation_start": serving_metadata["observation_start"],
        "observation_end": serving_metadata["observation_end"],
        "network_execution": False,
        "lens_activation": False,
        "production_activation": False,
        "oraclechambers_activation": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--instrument", required=True)
    args = parser.parse_args()
    print(json.dumps(publish_index_profile(args.instrument), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

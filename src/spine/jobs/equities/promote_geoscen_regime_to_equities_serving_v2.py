"""Governed, transaction-safe GeoScen-to-EQUITIES serving promotion."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import tempfile
import warnings

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_SOURCE = REPO_ROOT / "data/geoscen/llm/geoscen_rbl_regime_merged_v1.parquet"
DEFAULT_OUTPUT = REPO_ROOT / "data/serving/equities/equities_serving_v2.parquet"
DEFAULT_METADATA_OUTPUT = REPO_ROOT / "data/serving/equities/equities_serving_v2.metadata.json"

PRODUCER_VERSION = "1.1.0"
PROMOTION_METHOD_ID = "GEOSCEN_REGIME_TO_EQUITIES_SERVING_V2"
PROMOTION_METHOD_VERSION = "1.1.0"
UNIVERSE_ID = "SYSTEM_LEVEL_EQUITIES_REGIME"
HORIZON_ID = "HORIZON_NOT_FACTOR_WINDOW"
OBSERVATION_KEY = ("date",)
OUTPUT_COLUMNS = (
    "date",
    "rbl_report_with_regime",
    "regime_label",
    "regime_confidence",
    "dominance_mean",
    "signal_strength",
    "tone_direction",
)
TEXT_COLUMNS = ("rbl_report_with_regime", "regime_label")
NUMERIC_COLUMNS = ("regime_confidence", "dominance_mean", "signal_strength", "tone_direction")


class PromotionError(RuntimeError):
    """Base class for governed promotion failures."""


class CandidateValidationError(PromotionError):
    """Candidate creation or validation failed before publication."""


class PreexistingPairInconsistentError(PromotionError):
    """The fixed-name publication pair is incomplete or invalid."""


class PublicationError(PromotionError):
    """Publication failed and the prior state was restored."""


class PublicationPairValidationError(PublicationError):
    """The published target and metadata did not validate as a pair."""


class PublicationRollbackError(PromotionError):
    """Publication failed and rollback could not restore the prior state."""


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.columns.duplicated().any():
        duplicates = sorted(set(frame.columns[frame.columns.duplicated()].astype(str)))
        raise ValueError(f"EQUITIES_PROMOTION_DUPLICATE_COLUMNS:{','.join(duplicates)}")
    if tuple(frame.columns) != OUTPUT_COLUMNS:
        raise ValueError("EQUITIES_PROMOTION_OUTPUT_SCHEMA_INVALID")
    if frame.empty:
        raise ValueError("EQUITIES_PROMOTION_SOURCE_EMPTY")

    candidate = frame.copy()
    parsed_dates = pd.to_datetime(candidate["date"], errors="coerce", format="mixed")
    if parsed_dates.isna().any():
        raise ValueError("EQUITIES_PROMOTION_DATE_INVALID")
    candidate["date"] = parsed_dates

    for column in TEXT_COLUMNS:
        if candidate[column].isna().any():
            raise ValueError(f"EQUITIES_PROMOTION_TEXT_NULL:{column}")
        if candidate[column].map(lambda value: not isinstance(value, str) or not value.strip()).any():
            raise ValueError(f"EQUITIES_PROMOTION_TEXT_BLANK:{column}")

    for column in NUMERIC_COLUMNS:
        numeric = pd.to_numeric(candidate[column], errors="coerce")
        if numeric.isna().any():
            raise ValueError(f"EQUITIES_PROMOTION_NUMERIC_INVALID:{column}")
        if not numeric.map(lambda value: math.isfinite(float(value))).all():
            raise ValueError(f"EQUITIES_PROMOTION_NUMERIC_NONFINITE:{column}")
        candidate[column] = numeric.astype("float64")

    if candidate.duplicated(list(OBSERVATION_KEY)).any():
        raise ValueError("EQUITIES_PROMOTION_OBSERVATION_KEY_DUPLICATE:date")
    if not candidate["date"].is_monotonic_increasing:
        raise ValueError("EQUITIES_PROMOTION_ORDER_INVALID")
    return candidate


def _validate_and_project(source_path: Path) -> pd.DataFrame:
    if not source_path.is_file():
        raise FileNotFoundError(f"EQUITIES_PROMOTION_SOURCE_MISSING:{source_path}")
    if len(sha256_file(source_path)) != 64:
        raise ValueError("EQUITIES_PROMOTION_SOURCE_PROVENANCE_UNAVAILABLE")

    source = pd.read_parquet(source_path)
    if source.columns.duplicated().any():
        duplicates = sorted(set(source.columns[source.columns.duplicated()].astype(str)))
        raise ValueError(f"EQUITIES_PROMOTION_DUPLICATE_COLUMNS:{','.join(duplicates)}")
    missing = sorted(set(OUTPUT_COLUMNS) - set(source.columns))
    if missing:
        raise ValueError(f"EQUITIES_PROMOTION_REQUIRED_FIELDS_MISSING:{','.join(missing)}")
    if source.empty:
        raise ValueError("EQUITIES_PROMOTION_SOURCE_EMPTY")

    candidate = source.loc[:, list(OUTPUT_COLUMNS)].copy()
    candidate["date"] = pd.to_datetime(candidate["date"], errors="coerce", format="mixed")
    candidate = candidate.sort_values(list(OBSERVATION_KEY), kind="mergesort").reset_index(drop=True)
    return _validate_frame(candidate)


def _metadata(
    *,
    source_path: Path,
    output_path: Path,
    candidate: pd.DataFrame,
    target_sha256: str,
    generated_at_utc: str,
) -> dict[str, object]:
    return {
        "as_of_semantics": "OBSERVATION_DATE_ONLY_UPSTREAM_AVAILABILITY_NOT_PROVEN",
        "deterministic_row_ordering": "ASCENDING_BY_DATE_STABLE_MERGESORT",
        "generated_at_utc": generated_at_utc,
        "horizon_id": HORIZON_ID,
        "maximum_observation_date": candidate["date"].max().strftime("%Y-%m-%d"),
        "minimum_observation_date": candidate["date"].min().strftime("%Y-%m-%d"),
        "object_type": "EQUITIES_SYSTEM_LEVEL_REGIME_PROMOTION_METADATA_V1",
        "observation_key": list(OBSERVATION_KEY),
        "output_row_count": len(candidate),
        "output_schema": list(OUTPUT_COLUMNS),
        "producer_module": "spine.jobs.equities.promote_geoscen_regime_to_equities_serving_v2",
        "producer_version": PRODUCER_VERSION,
        "promotion_method_id": PROMOTION_METHOD_ID,
        "promotion_method_version": PROMOTION_METHOD_VERSION,
        "schema_version": "1.0.0",
        "source_artifact_path": source_path.resolve().as_posix(),
        "source_artifact_sha256": sha256_file(source_path),
        "source_required_field_contract": list(OUTPUT_COLUMNS),
        "source_row_count": len(candidate),
        "target_artifact_path": output_path.resolve().as_posix(),
        "target_artifact_sha256": target_sha256,
        "universe_id": UNIVERSE_ID,
        "universe_semantics": "SYSTEM_LEVEL_OBSERVATION_NO_SYMBOL_MEMBERSHIP_ASSERTED",
        "validation_status": "VALID",
    }


def _write_json_candidate(payload: dict[str, object], path: Path) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as stream:
        json.dump(payload, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write("\n")


def validate_publication_pair(
    target_path: str | Path,
    metadata_path: str | Path,
    *,
    expected_target_path: str | Path | None = None,
    expected_frame: pd.DataFrame | None = None,
) -> dict[str, object]:
    """Validate a fixed-name or candidate Parquet/metadata publication pair."""
    target_path = Path(target_path)
    metadata_path = Path(metadata_path)
    expected_target_path = Path(expected_target_path or target_path)
    if not target_path.is_file() or not metadata_path.is_file():
        raise ValueError("PUBLICATION_PAIR_MISSING")

    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("PUBLICATION_METADATA_INVALID_JSON") from exc

    actual = _validate_frame(pd.read_parquet(target_path))
    actual_hash = sha256_file(target_path)
    checks = {
        "target_artifact_path": expected_target_path.resolve().as_posix(),
        "target_artifact_sha256": actual_hash,
        "output_row_count": len(actual),
        "minimum_observation_date": actual["date"].min().strftime("%Y-%m-%d"),
        "maximum_observation_date": actual["date"].max().strftime("%Y-%m-%d"),
        "output_schema": list(actual.columns),
        "observation_key": list(OBSERVATION_KEY),
        "validation_status": "VALID",
    }
    mismatches = sorted(key for key, value in checks.items() if metadata.get(key) != value)
    if mismatches:
        raise ValueError(f"PUBLICATION_PAIR_METADATA_MISMATCH:{','.join(mismatches)}")
    if expected_frame is not None:
        pd.testing.assert_frame_equal(actual, expected_frame, check_dtype=True, check_exact=True)
    return metadata


def _adjacent_path(path: Path, suffix: str) -> Path:
    descriptor, name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=suffix)
    os.close(descriptor)
    return Path(name)


def _copy_backup(source: Path, backup: Path, phase: str) -> None:
    del phase
    shutil.copy2(source, backup)


def _replace_file(source: Path, destination: Path, phase: str) -> None:
    del phase
    os.replace(source, destination)


def _remove_if_present(path: Path) -> None:
    if path.exists():
        path.unlink()


def _restore_prior_state(
    *,
    target_path: Path,
    metadata_path: Path,
    target_backup: Path | None,
    metadata_backup: Path | None,
    prior_target_hash: str | None,
    prior_metadata_hash: str | None,
) -> None:
    if prior_target_hash is None:
        _remove_if_present(target_path)
    else:
        if target_backup is None or not target_backup.exists():
            raise RuntimeError("target backup unavailable")
        rollback_target = _adjacent_path(target_path, ".rollback")
        try:
            shutil.copy2(target_backup, rollback_target)
            _replace_file(rollback_target, target_path, "rollback_target")
        finally:
            _remove_if_present(rollback_target)

    if prior_metadata_hash is None:
        _remove_if_present(metadata_path)
    else:
        if metadata_backup is None or not metadata_backup.exists():
            raise RuntimeError("metadata backup unavailable")
        rollback_metadata = _adjacent_path(metadata_path, ".rollback")
        try:
            shutil.copy2(metadata_backup, rollback_metadata)
            _replace_file(rollback_metadata, metadata_path, "rollback_metadata")
        finally:
            _remove_if_present(rollback_metadata)

    if prior_target_hash is None or prior_metadata_hash is None:
        if target_path.exists() or metadata_path.exists():
            raise RuntimeError("prior absence not restored")
    else:
        if sha256_file(target_path) != prior_target_hash:
            raise RuntimeError("restored target hash mismatch")
        if sha256_file(metadata_path) != prior_metadata_hash:
            raise RuntimeError("restored metadata hash mismatch")
        validate_publication_pair(target_path, metadata_path)


def _cleanup(paths: list[Path | None], *, warn: bool = False) -> None:
    failures: list[str] = []
    for path in paths:
        if path is None:
            continue
        try:
            _remove_if_present(path)
        except OSError as exc:
            failures.append(f"{path}: {exc}")
    if failures:
        message = "PUBLICATION_CLEANUP_WARNING:" + " | ".join(failures)
        if warn:
            warnings.warn(message, RuntimeWarning, stacklevel=2)
        else:
            raise RuntimeError(message)


def promote(
    source_path: str | Path = DEFAULT_SOURCE,
    output_path: str | Path = DEFAULT_OUTPUT,
    metadata_output_path: str | Path = DEFAULT_METADATA_OUTPUT,
    *,
    generated_at_utc: str | None = None,
) -> dict[str, object]:
    source_path = Path(source_path)
    output_path = Path(output_path)
    metadata_output_path = Path(metadata_output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_output_path.parent.mkdir(parents=True, exist_ok=True)

    target_exists = output_path.exists()
    metadata_exists = metadata_output_path.exists()
    if target_exists != metadata_exists:
        raise PreexistingPairInconsistentError(
            f"PREEXISTING_PAIR_INCONSISTENT:target={target_exists}:metadata={metadata_exists}"
        )
    if target_exists:
        try:
            validate_publication_pair(output_path, metadata_output_path)
        except Exception as exc:
            raise PreexistingPairInconsistentError("PREEXISTING_PAIR_INCONSISTENT") from exc

    candidate_path: Path | None = None
    metadata_candidate: Path | None = None
    target_backup: Path | None = None
    metadata_backup: Path | None = None
    prior_target_hash = sha256_file(output_path) if target_exists else None
    prior_metadata_hash = sha256_file(metadata_output_path) if metadata_exists else None
    publication_started = False

    try:
        try:
            candidate = _validate_and_project(source_path)
            candidate_path = _adjacent_path(output_path, ".candidate.parquet")
            candidate.to_parquet(candidate_path, index=False)
            written = _validate_frame(pd.read_parquet(candidate_path))
            pd.testing.assert_frame_equal(written, candidate, check_dtype=True, check_exact=True)
            candidate_sha256 = sha256_file(candidate_path)
            generated_at_utc = generated_at_utc or datetime.now(timezone.utc).isoformat().replace(
                "+00:00", "Z"
            )
            metadata = _metadata(
                source_path=source_path,
                output_path=output_path,
                candidate=candidate,
                target_sha256=candidate_sha256,
                generated_at_utc=generated_at_utc,
            )
            metadata_candidate = _adjacent_path(metadata_output_path, ".candidate.json")
            _write_json_candidate(metadata, metadata_candidate)
            validate_publication_pair(
                candidate_path,
                metadata_candidate,
                expected_target_path=output_path,
                expected_frame=candidate,
            )
        except Exception as exc:
            raise CandidateValidationError(f"CANDIDATE_VALIDATION_FAILED:{exc}") from exc

        try:
            if target_exists:
                target_backup = _adjacent_path(output_path, ".bak")
                _copy_backup(output_path, target_backup, "backup_target")
                metadata_backup = _adjacent_path(metadata_output_path, ".bak")
                _copy_backup(metadata_output_path, metadata_backup, "backup_metadata")
        except Exception as exc:
            _cleanup([target_backup, metadata_backup])
            raise PublicationError(f"PUBLICATION_FAILED:backup:{exc}") from exc

        publication_started = True
        try:
            _replace_file(candidate_path, output_path, "publish_target")
            candidate_path = None
            _replace_file(metadata_candidate, metadata_output_path, "publish_metadata")
            metadata_candidate = None
            try:
                validate_publication_pair(
                    output_path,
                    metadata_output_path,
                    expected_frame=candidate,
                )
            except Exception as exc:
                raise PublicationPairValidationError(
                    f"PUBLICATION_PAIR_VALIDATION_FAILED:{exc}"
                ) from exc
        except Exception as publication_failure:
            try:
                _restore_prior_state(
                    target_path=output_path,
                    metadata_path=metadata_output_path,
                    target_backup=target_backup,
                    metadata_backup=metadata_backup,
                    prior_target_hash=prior_target_hash,
                    prior_metadata_hash=prior_metadata_hash,
                )
                _cleanup([target_backup, metadata_backup])
            except Exception as rollback_failure:
                raise PublicationRollbackError(
                    "PUBLICATION_ROLLBACK_FAILED:"
                    f"original={publication_failure!r}:rollback={rollback_failure!r}:"
                    f"target={output_path}:metadata={metadata_output_path}:"
                    f"target_present={output_path.exists()}:metadata_present={metadata_output_path.exists()}"
                ) from rollback_failure
            target_backup = metadata_backup = None
            raise PublicationError(f"PUBLICATION_FAILED:{publication_failure}") from publication_failure

        _cleanup([target_backup, metadata_backup], warn=True)
        target_backup = metadata_backup = None
        return metadata
    finally:
        residue = [candidate_path, metadata_candidate]
        if not publication_started:
            residue.extend([target_backup, metadata_backup])
        _cleanup(residue, warn=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--metadata-output", type=Path, default=DEFAULT_METADATA_OUTPUT)
    args = parser.parse_args()
    metadata = promote(args.source, args.output, args.metadata_output)
    print(json.dumps(metadata, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

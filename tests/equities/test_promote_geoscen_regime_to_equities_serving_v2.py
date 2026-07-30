import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from spine.jobs.equities import promote_geoscen_regime_to_equities_serving_v2 as producer
from spine.jobs.equities.promote_geoscen_regime_to_equities_serving_v2 import (
    OUTPUT_COLUMNS,
    PreexistingPairInconsistentError,
    PublicationError,
    PublicationRollbackError,
    _validate_and_project,
    promote,
    validate_publication_pair,
)


def _source_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": pd.Timestamp("2026-04-16"),
                "rbl_report_with_regime": "second report",
                "regime_label": "second label",
                "regime_confidence": 0.2,
                "dominance_mean": 0.3,
                "signal_strength": 0.4,
                "tone_direction": -0.5,
                "ignored_source_field": "not promoted",
            },
            {
                "date": pd.Timestamp("2026-03-19"),
                "rbl_report_with_regime": "first report",
                "regime_label": "first label",
                "regime_confidence": 0.1,
                "dominance_mean": 0.2,
                "signal_strength": 0.3,
                "tone_direction": 0.4,
                "ignored_source_field": "not promoted",
            },
        ]
    )


def _paths(tmp_path: Path) -> tuple[Path, Path, Path]:
    return (
        tmp_path / "source.parquet",
        tmp_path / "data/serving/equities/equities_serving_v2.parquet",
        tmp_path / "data/serving/equities/equities_serving_v2.metadata.json",
    )


def _run(tmp_path: Path, frame: pd.DataFrame | None = None):
    source, output, metadata = _paths(tmp_path)
    source.parent.mkdir(parents=True, exist_ok=True)
    (frame if frame is not None else _source_frame()).to_parquet(source, index=False)
    result = promote(
        source,
        output,
        metadata,
        generated_at_utc="2026-07-30T12:00:00Z",
    )
    return source, output, metadata, result


def test_valid_lossless_promotion_and_provenance(tmp_path: Path) -> None:
    source, output, metadata_path, metadata = _run(tmp_path)
    actual = pd.read_parquet(output)
    expected = _source_frame().loc[:, OUTPUT_COLUMNS].sort_values("date").reset_index(drop=True)

    assert tuple(actual.columns) == OUTPUT_COLUMNS
    assert "rbl_oc" not in actual
    pd.testing.assert_frame_equal(actual, expected, check_exact=True)
    assert actual["date"].is_monotonic_increasing
    assert metadata["source_row_count"] == metadata["output_row_count"] == 2
    assert metadata["observation_key"] == ["date"]
    assert metadata["universe_id"] == "SYSTEM_LEVEL_EQUITIES_REGIME"
    assert metadata["horizon_id"] == "HORIZON_NOT_FACTOR_WINDOW"
    assert metadata["source_artifact_sha256"] == hashlib.sha256(source.read_bytes()).hexdigest()
    assert metadata["target_artifact_sha256"] == hashlib.sha256(output.read_bytes()).hexdigest()
    assert json.loads(metadata_path.read_text()) == metadata


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        (lambda frame: frame.drop(columns=["regime_label"]), "REQUIRED_FIELDS_MISSING"),
        (lambda frame: frame.iloc[0:0], "SOURCE_EMPTY"),
        (lambda frame: frame.assign(date=["bad", "also-bad"]), "DATE_INVALID"),
        (lambda frame: frame.assign(regime_label=["", "ok"]), "TEXT_BLANK"),
        (lambda frame: frame.assign(signal_strength=[None, 0.2]), "NUMERIC_INVALID"),
        (lambda frame: frame.assign(signal_strength=["bad", "0.2"]), "NUMERIC_INVALID"),
        (lambda frame: frame.assign(signal_strength=[np.inf, 0.2]), "NUMERIC_NONFINITE"),
        (
            lambda frame: frame.assign(date=[pd.Timestamp("2026-03-19")] * 2),
            "OBSERVATION_KEY_DUPLICATE",
        ),
    ],
)
def test_invalid_sources_fail_closed_without_replacing_existing_target(
    tmp_path: Path, mutation, reason: str,
) -> None:
    source, output, metadata = _paths(tmp_path)
    _run(tmp_path)
    original = output.read_bytes()
    original_metadata = metadata.read_bytes()
    mutation(_source_frame()).to_parquet(source, index=False)

    with pytest.raises(producer.CandidateValidationError, match=reason):
        promote(source, output, metadata, generated_at_utc="2026-07-30T12:00:00Z")
    assert output.read_bytes() == original
    assert metadata.read_bytes() == original_metadata


def test_missing_source_fails_closed(tmp_path: Path) -> None:
    source, output, metadata = _paths(tmp_path)
    with pytest.raises(producer.CandidateValidationError, match="SOURCE_MISSING"):
        promote(source, output, metadata)
    assert not output.exists()


def test_output_is_compatible_with_historical_m_input_expectations(tmp_path: Path) -> None:
    _, output, _, _ = _run(tmp_path)
    frame = pd.read_parquet(output)
    numeric = [
        column
        for column in frame.columns
        if column != "date" and pd.api.types.is_numeric_dtype(frame[column])
    ]
    assert numeric == ["regime_confidence", "dominance_mean", "signal_strength", "tone_direction"]


def test_duplicate_columns_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, _, _ = _paths(tmp_path)
    source.write_bytes(b"synthetic parquet placeholder")
    frame = _source_frame()
    frame.insert(len(frame.columns), "regime_label_duplicate", frame["regime_label"])
    frame.columns = [*frame.columns[:-1], "regime_label"]
    monkeypatch.setattr(
        "spine.jobs.equities.promote_geoscen_regime_to_equities_serving_v2.pd.read_parquet",
        lambda _: frame,
    )
    with pytest.raises(ValueError, match="DUPLICATE_COLUMNS"):
        _validate_and_project(source)


def _assert_no_residue(tmp_path: Path) -> None:
    residue = [
        path
        for path in tmp_path.rglob("*")
        if path.is_file()
        and any(token in path.name for token in (".candidate.", ".bak", ".rollback", ".tmp"))
    ]
    assert residue == []


def _seed_old_pair(tmp_path: Path) -> tuple[Path, Path, Path, bytes, bytes]:
    source, output, metadata, _ = _run(tmp_path)
    return source, output, metadata, output.read_bytes(), metadata.read_bytes()


def test_pair_validator_accepts_complete_matching_pair(tmp_path: Path) -> None:
    _, output, metadata, _ = _run(tmp_path)
    validated = validate_publication_pair(output, metadata)
    assert validated["target_artifact_sha256"] == hashlib.sha256(output.read_bytes()).hexdigest()


@pytest.mark.parametrize("present", ["target", "metadata"])
def test_one_sided_preexisting_pair_fails_closed(tmp_path: Path, present: str) -> None:
    source, output, metadata = _paths(tmp_path)
    source.parent.mkdir(parents=True, exist_ok=True)
    _source_frame().to_parquet(source, index=False)
    path = output if present == "target" else metadata
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"orphan")
    with pytest.raises(PreexistingPairInconsistentError, match="PREEXISTING_PAIR_INCONSISTENT"):
        promote(source, output, metadata)
    assert path.read_bytes() == b"orphan"


def test_candidate_metadata_creation_failure_preserves_old_pair(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    source, output, metadata, old_target, old_metadata = _seed_old_pair(tmp_path)
    monkeypatch.setattr(producer, "_write_json_candidate", lambda *_: (_ for _ in ()).throw(OSError("boom")))
    with pytest.raises(producer.CandidateValidationError, match="CANDIDATE_VALIDATION_FAILED"):
        promote(source, output, metadata)
    assert output.read_bytes() == old_target
    assert metadata.read_bytes() == old_metadata
    _assert_no_residue(tmp_path)


def test_candidate_parquet_validation_failure_preserves_old_pair(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    source, output, metadata, old_target, old_metadata = _seed_old_pair(tmp_path)
    original = producer._validate_frame
    calls = 0

    def fail_written_candidate(frame):
        nonlocal calls
        calls += 1
        if calls == 3:
            raise ValueError("written candidate invalid")
        return original(frame)

    monkeypatch.setattr(producer, "_validate_frame", fail_written_candidate)
    with pytest.raises(producer.CandidateValidationError, match="CANDIDATE_VALIDATION_FAILED"):
        promote(source, output, metadata)
    assert output.read_bytes() == old_target
    assert metadata.read_bytes() == old_metadata
    _assert_no_residue(tmp_path)


def test_candidate_metadata_validation_failure_preserves_old_pair(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    source, output, metadata, old_target, old_metadata = _seed_old_pair(tmp_path)
    original = producer.validate_publication_pair

    def fail_candidate(target, meta, **kwargs):
        if str(target).endswith(".candidate.parquet"):
            raise ValueError("candidate metadata invalid")
        return original(target, meta, **kwargs)

    monkeypatch.setattr(producer, "validate_publication_pair", fail_candidate)
    with pytest.raises(producer.CandidateValidationError, match="CANDIDATE_VALIDATION_FAILED"):
        promote(source, output, metadata)
    assert output.read_bytes() == old_target
    assert metadata.read_bytes() == old_metadata
    _assert_no_residue(tmp_path)


@pytest.mark.parametrize("failed_phase", ["backup_target", "backup_metadata"])
def test_backup_failure_preserves_old_pair(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failed_phase: str,
) -> None:
    source, output, metadata, old_target, old_metadata = _seed_old_pair(tmp_path)
    original = producer._copy_backup

    def fail_selected(src, backup, phase):
        if phase == failed_phase:
            raise OSError(phase)
        return original(src, backup, phase)

    monkeypatch.setattr(producer, "_copy_backup", fail_selected)
    with pytest.raises(PublicationError, match="PUBLICATION_FAILED"):
        promote(source, output, metadata)
    assert output.read_bytes() == old_target
    assert metadata.read_bytes() == old_metadata
    _assert_no_residue(tmp_path)


@pytest.mark.parametrize("failed_phase", ["publish_target", "publish_metadata"])
def test_replacement_failure_rolls_back_exact_old_pair(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failed_phase: str,
) -> None:
    source, output, metadata, old_target, old_metadata = _seed_old_pair(tmp_path)
    original = producer._replace_file

    def fail_selected(src, destination, phase):
        if phase == failed_phase:
            raise OSError(phase)
        return original(src, destination, phase)

    monkeypatch.setattr(producer, "_replace_file", fail_selected)
    with pytest.raises(PublicationError, match="PUBLICATION_FAILED"):
        promote(source, output, metadata)
    assert output.read_bytes() == old_target
    assert metadata.read_bytes() == old_metadata
    validate_publication_pair(output, metadata)
    _assert_no_residue(tmp_path)


@pytest.mark.parametrize(
    "message",
    [
        "published target hash mismatch",
        "published metadata parse failure",
        "published metadata target-hash mismatch",
        "published row/date/schema mismatch",
    ],
)
def test_published_pair_validation_failure_rolls_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, message: str,
) -> None:
    source, output, metadata, old_target, old_metadata = _seed_old_pair(tmp_path)
    original = producer.validate_publication_pair
    fixed_pair_calls = 0

    def fail_published(target, meta, **kwargs):
        nonlocal fixed_pair_calls
        if Path(target) == output:
            fixed_pair_calls += 1
            if fixed_pair_calls == 2:
                raise ValueError(message)
        return original(target, meta, **kwargs)

    monkeypatch.setattr(producer, "validate_publication_pair", fail_published)
    with pytest.raises(PublicationError, match="PUBLICATION_FAILED"):
        promote(source, output, metadata)
    assert output.read_bytes() == old_target
    assert metadata.read_bytes() == old_metadata
    _assert_no_residue(tmp_path)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("target_artifact_sha256", "0" * 64),
        ("output_row_count", 999),
        ("minimum_observation_date", "1900-01-01"),
        ("maximum_observation_date", "2999-01-01"),
        ("output_schema", ["date"]),
        ("observation_key", ["date", "report"]),
        ("validation_status", "INVALID"),
    ],
)
def test_pair_validator_rejects_metadata_mismatch(
    tmp_path: Path, field: str, value,
) -> None:
    _, output, metadata, _ = _run(tmp_path)
    payload = json.loads(metadata.read_text())
    payload[field] = value
    metadata.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="PUBLICATION_PAIR_METADATA_MISMATCH"):
        validate_publication_pair(output, metadata)


def test_pair_validator_rejects_invalid_json(tmp_path: Path) -> None:
    _, output, metadata, _ = _run(tmp_path)
    metadata.write_text("{", encoding="utf-8")
    with pytest.raises(ValueError, match="PUBLICATION_METADATA_INVALID_JSON"):
        validate_publication_pair(output, metadata)


@pytest.mark.parametrize("rollback_phase", ["rollback_target", "rollback_metadata"])
def test_rollback_failure_is_distinct_and_preserves_context(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, rollback_phase: str,
) -> None:
    source, output, metadata, _, _ = _seed_old_pair(tmp_path)
    original = producer._replace_file

    def fail_selected(src, destination, phase):
        if phase in ("publish_metadata", rollback_phase):
            raise OSError(phase)
        return original(src, destination, phase)

    monkeypatch.setattr(producer, "_replace_file", fail_selected)
    with pytest.raises(PublicationRollbackError) as caught:
        promote(source, output, metadata)
    message = str(caught.value)
    assert "PUBLICATION_ROLLBACK_FAILED" in message
    assert "original=" in message and "rollback=" in message
    assert str(output) in message and str(metadata) in message


def test_failed_first_publication_restores_prior_absence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    source, output, metadata = _paths(tmp_path)
    source.parent.mkdir(parents=True, exist_ok=True)
    _source_frame().to_parquet(source, index=False)
    original = producer._replace_file

    def fail_metadata(src, destination, phase):
        if phase == "publish_metadata":
            raise OSError("metadata")
        return original(src, destination, phase)

    monkeypatch.setattr(producer, "_replace_file", fail_metadata)
    with pytest.raises(PublicationError):
        promote(source, output, metadata)
    assert not output.exists()
    assert not metadata.exists()
    _assert_no_residue(tmp_path)


def test_repeated_success_keeps_content_and_pair_valid(tmp_path: Path) -> None:
    _, output, metadata, _ = _run(tmp_path)
    first = pd.read_parquet(output)
    source, _, _, _ = _run(tmp_path)
    second = pd.read_parquet(output)
    pd.testing.assert_frame_equal(first, second, check_exact=True)
    validate_publication_pair(output, metadata)
    assert source.exists()
    _assert_no_residue(tmp_path)


def test_backup_cleanup_failure_leaves_valid_published_pair_and_warns(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    source, output, metadata, _, _ = _seed_old_pair(tmp_path)
    original = producer._remove_if_present

    def fail_backup_cleanup(path):
        if path.suffix == ".bak":
            raise OSError("cleanup")
        return original(path)

    monkeypatch.setattr(producer, "_remove_if_present", fail_backup_cleanup)
    with pytest.warns(RuntimeWarning, match="PUBLICATION_CLEANUP_WARNING"):
        promote(source, output, metadata)
    validate_publication_pair(output, metadata)

import json
import shutil
from pathlib import Path

import pandas as pd
import pytest

from spine.equities.index_pipeline import CANONICAL_COLUMNS
from spine.equities.index_profiles import REPO_ROOT, load_authorization, load_profile
from spine.equities.provenance.canonical_observation_metadata import (
    compute_canonical_observation_metadata_id,
)
from spine.jobs.equities import publish_index_profile_serving_v1 as job


CANONICAL = REPO_ROOT / "data/canonical/equities/indexes/qqq_daily_eod_v1.parquet"
CANONICAL_METADATA = CANONICAL.with_suffix(".metadata.json")


def _inputs():
    return job.verify_canonical_input("QQQ")


def _copy_input(tmp_path: Path):
    profile = load_profile("QQQ")
    auth_ref = profile["acquisition_policy"]["authorization_reference"]
    auth_target = tmp_path / auth_ref
    auth_target.parent.mkdir(parents=True)
    shutil.copy2(REPO_ROOT / auth_ref, auth_target)
    canonical_target = tmp_path / profile["serving_contract"]["canonical_path"]
    metadata_target = tmp_path / profile["serving_contract"]["canonical_metadata_path"]
    canonical_target.parent.mkdir(parents=True)
    shutil.copy2(CANONICAL, canonical_target)
    shutil.copy2(CANONICAL_METADATA, metadata_target)
    return canonical_target, metadata_target


def _publish_temp(tmp_path: Path):
    profile, auth, metadata, canonical, metadata_path, frame = _inputs()
    return profile, auth, metadata, canonical, metadata_path, frame, job.publish_pair(
        job.build_serving_candidate(frame), profile, auth, canonical, metadata_path,
        metadata, repo_root=tmp_path,
    )


def test_valid_qqq_serving_publication_and_schema():
    profile, auth, canonical_metadata, canonical, metadata_path, frame = _inputs()
    serving = job.build_serving_candidate(frame)
    assert len(serving) == 275
    assert list(serving.columns) == list(CANONICAL_COLUMNS)
    assert serving["symbol"].unique().tolist() == ["QQQ"]
    assert serving["date"].is_monotonic_increasing
    assert not serving.duplicated(["symbol", "date"]).any()
    assert not serving["close"].equals(serving["adj_close"])


@pytest.mark.parametrize(("remove", "error"), [
    ("canonical", "CANONICAL_MISSING"),
    ("metadata", "CANONICAL_METADATA_MISSING"),
])
def test_missing_canonical_pair_member(tmp_path, remove, error):
    canonical, metadata = _copy_input(tmp_path)
    (canonical if remove == "canonical" else metadata).unlink()
    with pytest.raises(ValueError, match=error):
        job.verify_canonical_input("QQQ", repo_root=tmp_path)


def test_canonical_hash_mismatch(tmp_path):
    canonical, _ = _copy_input(tmp_path)
    canonical.write_bytes(canonical.read_bytes() + b"x")
    with pytest.raises(ValueError, match="HASH_MISMATCH"):
        job.verify_canonical_input("QQQ", repo_root=tmp_path)


@pytest.mark.parametrize(("field", "value"), [
    ("instrument_id", "SPY"),
    ("provider", "OTHER"),
    ("provider_dataset", "OTHER"),
    ("authorization_id", "OTHER"),
    ("record_count", 274),
    ("observation_start", "2025-06-21T00:00:00Z"),
])
def test_canonical_metadata_identity_mismatch(tmp_path, field, value):
    _, metadata_path = _copy_input(tmp_path)
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata[field] = value
    metadata["metadata_id"] = compute_canonical_observation_metadata_id(metadata)
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
    with pytest.raises(ValueError, match=field):
        job.verify_canonical_input("QQQ", repo_root=tmp_path)


def test_duplicate_key_rejected(tmp_path):
    canonical, metadata_path = _copy_input(tmp_path)
    frame = pd.read_parquet(canonical)
    frame.loc[1, "date"] = frame.loc[0, "date"]
    frame.to_parquet(canonical, index=False)
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["canonical_artifact_sha256"] = job._hash(canonical)
    metadata["metadata_id"] = compute_canonical_observation_metadata_id(metadata)
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
    with pytest.raises(ValueError, match="canonical_observation_key"):
        job.verify_canonical_input("QQQ", repo_root=tmp_path)


def test_serving_metadata_declares_lineage_and_boundaries():
    path = REPO_ROOT / "data/serving/equities/indexes/qqq_daily_eod_v1.metadata.json"
    metadata = json.loads(path.read_text(encoding="utf-8"))
    assert metadata["retained_fields"] == list(CANONICAL_COLUMNS)
    assert metadata["omitted_fields"] == []
    assert metadata["serving_artifact_sha256"] == job._hash(
        REPO_ROOT / "data/serving/equities/indexes/qqq_daily_eod_v1.parquet"
    )
    assert metadata["source_canonical_artifact_sha256"] == job._hash(CANONICAL)
    assert metadata["serving_publication"] == "COMPLETE"
    assert metadata["lifecycle"] == "SHADOW"
    assert metadata["runtime_state"] == "VALIDATION_ONLY"
    assert metadata["lens_eligibility"] == "PENDING"
    assert metadata["production_publication"] == "PROHIBITED"
    assert metadata["oraclechambers_eligibility"] == "NOT_AUTHORIZED"


@pytest.mark.parametrize("instrument", ["SPY", "DIA", "IWM", "MDY", "ITOT"])
def test_other_instrument_path_collision_rejected(tmp_path, monkeypatch, instrument):
    profile = load_profile("QQQ")
    other = load_profile(instrument)
    profile["serving_contract"]["serving_path"] = other["serving_contract"]["serving_path"]
    monkeypatch.setattr(job, "load_all_profiles", lambda: [profile, other])
    with pytest.raises(ValueError, match="PATH_COLLISION"):
        job._validate_destinations(profile, tmp_path)


def test_aggregate_path_rejected(tmp_path):
    profile = load_profile("QQQ")
    profile["serving_contract"]["serving_path"] = (
        "data/serving/equities/us_equity_index_data.json"
    )
    with pytest.raises(ValueError, match="AGGREGATE"):
        job._validate_destinations(profile, tmp_path)


@pytest.mark.parametrize("failed_phase", ["publish_serving", "publish_metadata"])
def test_replacement_failure_rolls_back_pair(tmp_path, monkeypatch, failed_phase):
    profile, auth, metadata, canonical, metadata_path, frame, result = _publish_temp(tmp_path)
    serving, serving_metadata, _ = result
    before = (serving.read_bytes(), serving_metadata.read_bytes())
    original = job._replace

    def fail(source, destination, phase):
        if phase == failed_phase:
            raise OSError(failed_phase)
        return original(source, destination, phase)

    monkeypatch.setattr(job, "_replace", fail)
    with pytest.raises(OSError, match=failed_phase):
        job.publish_pair(
            job.build_serving_candidate(frame), profile, auth, canonical,
            metadata_path, metadata, repo_root=tmp_path,
        )
    assert (serving.read_bytes(), serving_metadata.read_bytes()) == before
    assert not [path for path in serving.parent.iterdir() if path.name.startswith(".")]


def test_final_validation_failure_rolls_back_pair(tmp_path):
    profile, auth, metadata, canonical, metadata_path, frame, result = _publish_temp(tmp_path)
    serving, serving_metadata, _ = result
    before = (serving.read_bytes(), serving_metadata.read_bytes())
    calls = 0

    def fail_final(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 3:
            raise ValueError("FINAL_VALIDATION")
        return job.validate_serving_pair(*args, **kwargs)

    with pytest.raises(ValueError, match="FINAL_VALIDATION"):
        job.publish_pair(
            job.build_serving_candidate(frame), profile, auth, canonical,
            metadata_path, metadata, repo_root=tmp_path, validator=fail_final,
        )
    assert (serving.read_bytes(), serving_metadata.read_bytes()) == before


def test_inconsistent_existing_pair_rejected(tmp_path):
    profile, auth, metadata, canonical, metadata_path, frame = _inputs()
    serving = tmp_path / profile["serving_contract"]["serving_path"]
    serving.parent.mkdir(parents=True)
    serving.write_bytes(b"orphan")
    with pytest.raises(ValueError, match="PREEXISTING_PAIR_INCONSISTENT"):
        job.publish_pair(
            job.build_serving_candidate(frame), profile, auth, canonical,
            metadata_path, metadata, repo_root=tmp_path,
        )


def test_no_network_or_activation_path(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("network called")

    import requests
    monkeypatch.setattr(requests, "get", forbidden)
    result = job.publish_index_profile("QQQ")
    assert result["network_execution"] is False
    assert result["lens_activation"] is False
    assert result["production_activation"] is False
    assert result["oraclechambers_activation"] is False


def test_second_publication_is_deterministic():
    first = job.publish_index_profile("QQQ")["serving_artifact_sha256"]
    second = job.publish_index_profile("QQQ")["serving_artifact_sha256"]
    assert first == second

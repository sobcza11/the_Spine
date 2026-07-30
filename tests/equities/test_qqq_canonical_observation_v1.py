import json
import shutil
from pathlib import Path

import pandas as pd
import pytest

from spine.equities.index_pipeline import CANONICAL_COLUMNS, canonicalize_tiingo_daily
from spine.equities.index_profiles import REPO_ROOT, load_authorization, load_profile
from spine.jobs.equities import canonicalize_index_profile_v1 as job


RAW = REPO_ROOT / "data/raw/equities/indexes/qqq_daily_eod_2025-06-20_2026-07-24.json"
MANIFEST = RAW.with_suffix(".acquisition.json")


def _rows():
    return json.loads(RAW.read_text(encoding="utf-8"))


def _authorization():
    profile = load_profile("QQQ")
    return load_authorization(profile["acquisition_policy"]["authorization_reference"])


def _frame(rows=None):
    return job.build_canonical_candidate("QQQ", rows or _rows(), _authorization())


def _temp_input(tmp_path: Path):
    authorization = _authorization()
    authorization_source = (
        REPO_ROOT / load_profile("QQQ")["acquisition_policy"]["authorization_reference"]
    )
    authorization_target = (
        tmp_path / load_profile("QQQ")["acquisition_policy"]["authorization_reference"]
    )
    authorization_target.parent.mkdir(parents=True)
    shutil.copy2(authorization_source, authorization_target)
    raw = tmp_path / authorization["raw_destination"]
    manifest = tmp_path / authorization["acquisition_manifest_destination"]
    raw.parent.mkdir(parents=True)
    shutil.copy2(RAW, raw)
    shutil.copy2(MANIFEST, manifest)
    return raw, manifest


def test_valid_qqq_canonicalization_and_contract():
    frame = _frame()
    assert len(frame) == 275
    assert list(frame.columns) == list(CANONICAL_COLUMNS)
    assert frame["symbol"].unique().tolist() == ["QQQ"]
    assert frame["date"].min().date().isoformat() == "2025-06-20"
    assert frame["date"].max().date().isoformat() == "2026-07-24"
    assert not frame.duplicated(["symbol", "date"]).any()
    assert frame.equals(frame.sort_values(["symbol", "date"]).reset_index(drop=True))
    assert not frame["close"].equals(frame["adj_close"])


@pytest.mark.parametrize("field", ["open", "adjOpen"])
def test_missing_raw_or_adjusted_field(field):
    rows = _rows()
    rows[0].pop(field)
    with pytest.raises(ValueError, match="FIELD_MISSING"):
        _frame(rows)


@pytest.mark.parametrize(
    ("field", "value", "error"),
    [
        ("close", 0, "PRICE_INVALID"),
        ("adjClose", -1, "PRICE_INVALID"),
        ("volume", -1, "VOLUME_INVALID"),
        ("adjVolume", -1, "VOLUME_INVALID"),
        ("splitFactor", 0, "SPLIT_FACTOR_INVALID"),
    ],
)
def test_invalid_values_fail_closed(field, value, error):
    rows = _rows()
    rows[0][field] = value
    with pytest.raises(ValueError, match=error):
        _frame(rows)


def test_duplicate_and_out_of_range_dates_fail_closed():
    duplicate = _rows()
    duplicate[1]["date"] = duplicate[0]["date"]
    with pytest.raises(ValueError, match="OBSERVATION_KEY_DUPLICATE"):
        _frame(duplicate)
    outside = _rows()
    outside[0]["date"] = "2025-06-19T00:00:00Z"
    with pytest.raises(ValueError, match="DATE_OUTSIDE_AUTHORIZATION"):
        _frame(outside)


def test_deterministic_row_and_column_ordering():
    rows = list(reversed(_rows()))
    first = _frame(rows)
    second = _frame(rows)
    pd.testing.assert_frame_equal(first, second)
    assert list(first.columns) == list(CANONICAL_COLUMNS)
    assert first["date"].is_monotonic_increasing


@pytest.mark.parametrize(
    ("mutation", "error"),
    [
        (lambda raw, manifest: raw.write_text("[]", encoding="utf-8"), "raw_artifact_sha256"),
        (
            lambda raw, manifest: _mutate_json(manifest, "symbols", ["SPY"]),
            "symbols",
        ),
        (
            lambda raw, manifest: _mutate_json(manifest, "provider", "OTHER"),
            "provider",
        ),
        (
            lambda raw, manifest: _mutate_json(manifest, "provider_dataset", "OTHER"),
            "provider_dataset",
        ),
    ],
)
def test_input_evidence_mismatches(tmp_path, mutation, error):
    raw, manifest = _temp_input(tmp_path)
    mutation(raw, manifest)
    with pytest.raises(ValueError, match=error):
        job.verify_inputs("QQQ", repo_root=tmp_path)


def _mutate_json(path: Path, key: str, value):
    document = json.loads(path.read_text(encoding="utf-8"))
    document[key] = value
    path.write_text(json.dumps(document), encoding="utf-8")


def test_metadata_records_governed_lineage():
    metadata_path = REPO_ROOT / "data/canonical/equities/indexes/qqq_daily_eod_v1.metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    authorization = _authorization()
    assert metadata["canonical_artifact_sha256"] == job._hash(
        REPO_ROOT / "data/canonical/equities/indexes/qqq_daily_eod_v1.parquet"
    )
    assert metadata["source_raw_artifact_sha256"] == manifest["raw_artifact_sha256"]
    assert metadata["source_acquisition_manifest_id"] == manifest["manifest_id"]
    assert metadata["authorization_id"] == authorization["authorization_id"]
    assert metadata["evidence"] == [{
        "artifact": str(
            REPO_ROOT / "data/canonical/equities/indexes/qqq_daily_eod_v1.parquet"
        )
    }]
    assert metadata["serving_status"] == "NOT_PUBLISHED"
    assert metadata["lens_eligibility_status"] == "PENDING_NO_ACTIVATION"
    assert metadata["production_status"] == "NOT_ACTIVATED"


def _publish_temp(tmp_path: Path):
    profile = load_profile("QQQ")
    authorization = _authorization()
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    frame = _frame()
    return profile, authorization, manifest, frame, job.publish_pair(
        frame, profile, authorization, manifest, RAW, repo_root=tmp_path
    )


@pytest.mark.parametrize("failed_phase", ["publish_canonical", "publish_metadata"])
def test_replacement_failure_rolls_back_pair(tmp_path, monkeypatch, failed_phase):
    profile, authorization, manifest, frame, result = _publish_temp(tmp_path)
    canonical, metadata, _ = result
    before = (canonical.read_bytes(), metadata.read_bytes())
    original = job._replace

    def fail(source, destination, phase):
        if phase == failed_phase:
            raise OSError(failed_phase)
        return original(source, destination, phase)

    monkeypatch.setattr(job, "_replace", fail)
    with pytest.raises(OSError, match=failed_phase):
        job.publish_pair(frame, profile, authorization, manifest, RAW, repo_root=tmp_path)
    assert (canonical.read_bytes(), metadata.read_bytes()) == before
    assert not list(canonical.parent.glob(".*.candidate"))
    assert not list(canonical.parent.glob(".*.bak"))
    assert not list(canonical.parent.glob(".*.rollback"))


def test_final_validation_failure_rolls_back_pair(tmp_path):
    profile, authorization, manifest, frame, result = _publish_temp(tmp_path)
    canonical, metadata, _ = result
    before = (canonical.read_bytes(), metadata.read_bytes())
    calls = 0

    def fail_final(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 3:
            raise ValueError("FINAL_VALIDATION")
        return job.validate_pair(*args, **kwargs)

    with pytest.raises(ValueError, match="FINAL_VALIDATION"):
        job.publish_pair(
            frame, profile, authorization, manifest, RAW,
            repo_root=tmp_path, validator=fail_final,
        )
    assert (canonical.read_bytes(), metadata.read_bytes()) == before


def test_qqq_path_cannot_overwrite_spy(tmp_path):
    profile = load_profile("QQQ")
    profile["serving_contract"]["canonical_path"] = (
        "data/canonical/equities/indexes/spy_daily_eod_v1.parquet"
    )
    with pytest.raises(ValueError, match="SPY_PATH_COLLISION"):
        job.publish_pair(
            _frame(), profile, _authorization(),
            json.loads(MANIFEST.read_text(encoding="utf-8")), RAW, repo_root=tmp_path,
        )


def test_canonical_stage_does_not_authorize_lens_or_production():
    profile = load_profile("QQQ")
    assert profile["lens_eligibility"]["policy"] == "EVALUATE_ONLY_NO_ACTIVATION"
    assert profile["runtime_authorization"] == "VALIDATION_ONLY"
    canonical_metadata = json.loads(
        (REPO_ROOT / profile["serving_contract"]["canonical_metadata_path"]).read_text(
            encoding="utf-8"
        )
    )
    assert canonical_metadata["lens_eligibility_status"] == "PENDING_NO_ACTIVATION"
    assert canonical_metadata["production_status"] == "NOT_ACTIVATED"

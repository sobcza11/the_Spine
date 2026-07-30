import json
import shutil
from pathlib import Path

import pandas as pd

from spine.equities.index_profiles import REPO_ROOT
from spine.jobs.equities.check_index_lens_eligibility_v1 import (
    OBJECT_TYPE,
    check_qqq_lens_eligibility,
    evaluate_qqq_lens_eligibility,
)


SERVING = REPO_ROOT / "data/serving/equities/indexes/qqq_daily_eod_v1.parquet"
METADATA = SERVING.with_suffix(".metadata.json")
CANONICAL = REPO_ROOT / "data/canonical/equities/indexes/qqq_daily_eod_v1.parquet"
PROFILE = REPO_ROOT / "config/equities/index_profiles/qqq.json"


def _fixture(tmp_path: Path):
    serving = tmp_path / "serving.parquet"
    metadata = tmp_path / "serving.metadata.json"
    canonical = tmp_path / "canonical.parquet"
    profile = tmp_path / "qqq.json"
    shutil.copy2(SERVING, serving)
    shutil.copy2(METADATA, metadata)
    shutil.copy2(CANONICAL, canonical)
    shutil.copy2(PROFILE, profile)
    document = json.loads(metadata.read_text(encoding="utf-8"))
    document["source_canonical_artifact_sha256"] = _hash(canonical)
    metadata.write_text(json.dumps(document), encoding="utf-8")
    return serving, metadata, canonical, profile


def _hash(path: Path):
    import hashlib
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _evaluate(paths):
    serving, metadata, canonical, profile = paths
    return evaluate_qqq_lens_eligibility(
        serving_path=serving,
        metadata_path=metadata,
        canonical_path=canonical,
        profile_path=profile,
    )


def test_current_qqq_is_blocked_pending_governed_lens_contracts():
    result = check_qqq_lens_eligibility()
    assert result["object_type"] == OBJECT_TYPE
    assert result["eligible_lenses"] == []
    assert result["blocked_lenses"] == [
        "MARKET_BREADTH", "VOLATILITY_STRUCTURE", "LIQUIDITY_FLOWS"
    ]
    assert result["record_count"] == 275
    assert result["observation_range"] == {
        "start": "2025-06-20", "end": "2026-07-24"
    }
    assert result["runtime_state"] == "VALIDATION_ONLY"
    assert result["publication_state"] == "COMPLETE"
    assert result["activation_performed"] is False
    assert "HISTORY_REQUIREMENT_UNPROVEN" in result["reason_codes"]


def test_wrong_symbol_and_spy_contamination(tmp_path):
    paths = _fixture(tmp_path)
    frame = pd.read_parquet(paths[0])
    frame.loc[0, "symbol"] = "SPY"
    frame.to_parquet(paths[0], index=False)
    result = _evaluate(paths)
    assert "SYMBOL_IDENTITY_INVALID" in result["reason_codes"]
    assert "SPY_CONTAMINATION" in result["reason_codes"]
    assert "CROSS_INDEX_ROWS" in result["reason_codes"]


def test_duplicate_dates_are_rejected(tmp_path):
    paths = _fixture(tmp_path)
    frame = pd.read_parquet(paths[0])
    frame.loc[1, "date"] = frame.loc[0, "date"]
    frame.to_parquet(paths[0], index=False)
    assert "DUPLICATE_OBSERVATION_KEY" in _evaluate(paths)["reason_codes"]


def test_missing_metadata_fails_closed(tmp_path):
    paths = _fixture(tmp_path)
    paths[1].unlink()
    result = _evaluate(paths)
    assert result["eligible_lenses"] == []
    assert "SERVING_METADATA_MISSING" in result["reason_codes"]


def test_invalid_hash_fails_closed(tmp_path):
    paths = _fixture(tmp_path)
    frame = pd.read_parquet(paths[0])
    frame.loc[0, "close"] += 1
    frame.to_parquet(paths[0], index=False)
    assert "SERVING_ARTIFACT_HASH_MISMATCH" in _evaluate(paths)["reason_codes"]


def test_insufficient_history_is_not_inferred(tmp_path):
    paths = _fixture(tmp_path)
    frame = pd.read_parquet(paths[0]).tail(2).reset_index(drop=True)
    frame.to_parquet(paths[0], index=False)
    metadata = json.loads(paths[1].read_text(encoding="utf-8"))
    metadata["serving_artifact_sha256"] = _hash(paths[0])
    metadata["observation_count"] = 2
    metadata["observation_start"] = frame["date"].min().strftime("%Y-%m-%dT00:00:00Z")
    paths[1].write_text(json.dumps(metadata), encoding="utf-8")
    result = _evaluate(paths)
    assert "HISTORY_REQUIREMENT_UNPROVEN" in result["reason_codes"]
    assert result["eligible_lenses"] == []


def test_output_is_deterministic():
    first = check_qqq_lens_eligibility()
    second = check_qqq_lens_eligibility()
    assert first == second

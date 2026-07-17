from __future__ import annotations

import hashlib

import pytest

from spine.jobs.geoscen.eis.exceptions import RawStorageError
from spine.jobs.geoscen.eis.raw_store import preserve_raw_response


def test_disabled_raw_store_returns_none(tmp_path) -> None:
    assert preserve_raw_response(
        provider="fred",
        operation="latest",
        content=b"raw",
        destination=tmp_path,
        preserve_raw=False,
    ) is None
    assert preserve_raw_response(
        provider="fred",
        operation="latest",
        content=b"raw",
        destination=None,
        preserve_raw=True,
    ) is None


def test_successful_atomic_write_and_sha(tmp_path) -> None:
    ref = preserve_raw_response(
        provider="fred",
        operation="latest",
        content=b"raw",
        destination=tmp_path,
        preserve_raw=True,
        content_type="application/json",
    )
    assert ref is not None
    assert ref.sha256 == hashlib.sha256(b"raw").hexdigest()
    assert ref.size_bytes == 3
    assert "fred-latest" in ref.path


def test_path_and_overwrite_rejection(tmp_path) -> None:
    with pytest.raises(RawStorageError):
        preserve_raw_response(
            provider="../bad",
            operation="latest",
            content=b"raw",
            destination=tmp_path,
            preserve_raw=True,
        )
    preserve_raw_response(provider="fred", operation="latest", content=b"raw", destination=tmp_path, preserve_raw=True)
    with pytest.raises(RawStorageError):
        preserve_raw_response(provider="fred", operation="latest", content=b"raw", destination=tmp_path, preserve_raw=True)

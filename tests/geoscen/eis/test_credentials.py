from __future__ import annotations

import pytest

from spine.jobs.geoscen.eis.credentials import (
    SECRET_MARKER,
    get_credential,
    get_provider_credentials,
    redact_mapping,
    redact_url,
)
from spine.jobs.geoscen.eis.exceptions import CredentialError


def test_required_credential_present(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FRED_API_KEY", "  abc123  ")
    credential = get_credential("FRED_API_KEY")
    assert credential is not None
    assert credential.value == "abc123"
    assert "abc123" not in repr(credential)


def test_required_credential_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    with pytest.raises(CredentialError) as exc:
        get_credential("FRED_API_KEY")
    assert "FRED_API_KEY" in str(exc.value)
    assert "secret" not in str(exc.value).lower()


def test_optional_credential_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OPTIONAL_KEY", raising=False)
    assert get_credential("OPTIONAL_KEY", required=False) is None


def test_provider_credentials_and_redaction(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BLS_API_KEY", "bls-secret")
    credentials = get_provider_credentials("bls", {"BLS_API_KEY": True, "OTHER": False})
    assert list(credentials) == ["BLS_API_KEY"]
    redacted = redact_mapping({"api_key": "abc", "nested": {"Authorization": "Bearer xyz"}, "safe": "ok"})
    assert redacted["api_key"] == SECRET_MARKER
    assert redacted["nested"]["Authorization"] == SECRET_MARKER
    assert redacted["safe"] == "ok"
    assert "abc" not in redact_url("https://example.com?a=1&api_key=abc")

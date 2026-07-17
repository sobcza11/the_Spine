from __future__ import annotations

from dataclasses import dataclass

import pytest
import requests

from spine.jobs.geoscen.eis.contracts import TimeoutPolicy
from spine.jobs.geoscen.eis.exceptions import RequestValidationError, UpstreamRateLimitError
from spine.jobs.geoscen.eis.http_client import (
    DEFAULT_USER_AGENT,
    GovernedHttpClient,
    HttpPolicy,
    build_session,
)


@dataclass
class FakeElapsed:
    def total_seconds(self) -> float:
        return 0.01


class FakeResponse:
    status_code = 200
    headers = {"content-type": "application/json"}
    content = b'{"ok":true}'
    url = "https://example.com/data?api_key=secret"
    elapsed = FakeElapsed()


class FakeSession:
    def __init__(self, status_code: int = 200) -> None:
        self.status_code = status_code
        self.calls = []

    def request(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        response = FakeResponse()
        response.status_code = self.status_code
        return response


def test_session_retry_policy() -> None:
    session = build_session(HttpPolicy(total_retries=4))
    adapter = session.get_adapter("https://example.com")
    retry = adapter.max_retries
    assert retry.total == 4
    assert 429 in retry.status_forcelist
    assert "GET" in retry.allowed_methods
    assert "PUT" not in retry.allowed_methods


def test_client_timeout_and_correlation_header() -> None:
    fake = FakeSession()
    client = GovernedHttpClient(session=fake)
    response = client.request(
        "GET",
        "https://example.com/data",
        correlation_id="corr-1",
        timeout_policy=TimeoutPolicy(connect_seconds=1, read_seconds=2),
    )
    args, kwargs = fake.calls[0]
    assert args[:2] == ("GET", "https://example.com/data")
    assert kwargs["timeout"] == (1, 2)
    assert kwargs["headers"]["X-Correlation-ID"] == "corr-1"
    assert kwargs["headers"]["User-Agent"] == DEFAULT_USER_AGENT
    assert response.url.endswith("api_key=%5Bredacted%5D")


def test_429_maps_to_rate_limit() -> None:
    client = GovernedHttpClient(session=FakeSession(status_code=429))
    with pytest.raises(UpstreamRateLimitError):
        client.request("GET", "https://example.com", correlation_id="c")


def test_unsafe_method_rejected() -> None:
    client = GovernedHttpClient(session=FakeSession())
    with pytest.raises(RequestValidationError):
        client.request("DELETE", "https://example.com", correlation_id="c")

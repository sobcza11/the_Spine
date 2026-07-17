from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from spine.jobs.geoscen.eis.contracts import TimeoutPolicy, UpstreamResponse, utc_now_iso
from spine.jobs.geoscen.eis.credentials import redact_mapping, redact_url
from spine.jobs.geoscen.eis.exceptions import (
    RequestValidationError,
    UpstreamAuthenticationError,
    UpstreamRateLimitError,
    UpstreamRequestError,
)

DEFAULT_USER_AGENT = "the_Spine GeoScen EIS/1.0"
ALLOWED_METHODS = frozenset({"GET", "POST"})
RETRY_METHODS = frozenset({"GET", "POST"})
RETRY_STATUSES = (429, 500, 502, 503, 504)


@dataclass(frozen=True)
class HttpPolicy:
    timeout: TimeoutPolicy = TimeoutPolicy()
    total_retries: int = 3
    backoff_factor: float = 0.5
    status_forcelist: tuple[int, ...] = RETRY_STATUSES


class GovernedHttpClient:
    def __init__(self, session: requests.Session | None = None, policy: HttpPolicy | None = None) -> None:
        self.policy = policy or HttpPolicy()
        self.session = session or build_session(self.policy)

    def request(
        self,
        method: str,
        url: str,
        *,
        correlation_id: str,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, object] | None = None,
        data: object = None,
        json: object = None,
        timeout_policy: TimeoutPolicy | None = None,
    ) -> UpstreamResponse:
        method_u = method.upper()
        if method_u not in ALLOWED_METHODS:
            raise RequestValidationError("Unsupported HTTP method.", context={"method": method_u})
        safe_headers = {
            "User-Agent": DEFAULT_USER_AGENT,
            "X-Correlation-ID": correlation_id,
            **dict(headers or {}),
        }
        timeout = (timeout_policy or self.policy.timeout).as_tuple()
        try:
            response = self.session.request(
                method_u,
                url,
                headers=safe_headers,
                params=params,
                data=data,
                json=json,
                timeout=timeout,
            )
        except requests.RequestException as exc:
            raise UpstreamRequestError(
                "Upstream request failed.",
                context={"error_type": type(exc).__name__, "url": redact_url(url)},
            ) from exc
        if response.status_code == 429:
            raise UpstreamRateLimitError(upstream_status=response.status_code)
        if response.status_code in {401, 403}:
            raise UpstreamAuthenticationError(upstream_status=response.status_code)
        if response.status_code >= 400:
            raise UpstreamRequestError("Upstream returned an error status.", upstream_status=response.status_code)
        return UpstreamResponse(
            url=redact_url(response.url),
            method=method_u,
            status_code=response.status_code,
            headers=dict(response.headers),
            content=response.content,
            retrieved_at=utc_now_iso(),
            elapsed_seconds=response.elapsed.total_seconds() if response.elapsed else None,
        )

    def safe_request_metadata(self, method: str, url: str, params: Mapping[str, object] | None = None) -> dict[str, object]:
        return {"method": method.upper(), "url": redact_url(url), "params": redact_mapping(params or {})}


def build_session(policy: HttpPolicy | None = None) -> requests.Session:
    policy = policy or HttpPolicy()
    session = requests.Session()
    retry = Retry(
        total=policy.total_retries,
        connect=policy.total_retries,
        read=policy.total_retries,
        status=policy.total_retries,
        backoff_factor=policy.backoff_factor,
        status_forcelist=policy.status_forcelist,
        allowed_methods=RETRY_METHODS,
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

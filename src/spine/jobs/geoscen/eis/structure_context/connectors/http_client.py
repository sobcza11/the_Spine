from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Mapping
from urllib.parse import urlencode
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class HttpResponse:
    url: str
    status: int
    payload: Any


def fetch_json(
    base_url: str,
    *,
    path: str = "",
    params: Mapping[str, Any] | None = None,
    headers: Mapping[str, str] | None = None,
    timeout_seconds: int | None = None,
) -> HttpResponse:
    if not base_url:
        raise ValueError("base_url is required")

    timeout = timeout_seconds or int(
        os.getenv("STRUCTURE_CONTEXT_HTTP_TIMEOUT_SECONDS", "30")
    )

    url = f"{base_url.rstrip('/')}/{path.lstrip('/')}" if path else base_url

    clean_params = {
        key: value
        for key, value in (params or {}).items()
        if value is not None and value != ""
    }

    if clean_params:
        url = f"{url}?{urlencode(clean_params, doseq=True)}"

    request_headers = {
        "Accept": "application/json",
        "User-Agent": os.getenv(
            "STRUCTURE_CONTEXT_USER_AGENT",
            "IsoVector-EIS/1.0",
        ),
        **dict(headers or {}),
    }

    request = Request(url, headers=request_headers, method="GET")

    with urlopen(request, timeout=timeout) as response:
        raw = response.read().decode("utf-8")
        payload = json.loads(raw)

        return HttpResponse(
            url=url,
            status=response.status,
            payload=payload,
        )

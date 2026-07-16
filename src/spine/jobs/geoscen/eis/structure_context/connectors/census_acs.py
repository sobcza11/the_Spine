from __future__ import annotations

import os
from typing import Any

from .http_client import fetch_json


DEFAULT_VARIABLES = [
    "NAME",
    "B01003_001E",
    "B19013_001E",
    "B15003_022E",
    "B17001_002E",
    "B25077_001E",
]


def fetch_acs_zcta(
    year: int,
    *,
    variables: list[str] | None = None,
    zcta: str = "*",
) -> dict[str, Any]:
    base_url = os.getenv(
        "CENSUS_API_BASE",
        "https://api.census.gov/data",
    )
    api_key = os.getenv("CENSUS_API_KEY", "")

    params: dict[str, Any] = {
        "get": ",".join(variables or DEFAULT_VARIABLES),
        "for": f"zip code tabulation area:{zcta}",
    }

    if api_key:
        params["key"] = api_key

    response = fetch_json(
        base_url,
        path=f"{year}/acs/acs5",
        params=params,
    )

    return {
        "source": "census_acs",
        "year": year,
        "geography": "zcta",
        "request_url": response.url,
        "status": response.status,
        "payload": response.payload,
    }
